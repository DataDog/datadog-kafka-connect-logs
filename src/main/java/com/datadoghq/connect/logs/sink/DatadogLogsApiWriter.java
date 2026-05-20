/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

import com.datadoghq.connect.logs.util.Project;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.ParseException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

public class DatadogLogsApiWriter implements Closeable {
    public static final int MAXIMUM_BATCH_BYTES = 4500000;

    // Matches the Datadog Agent's logs_config.http_timeout default (10s), which is the total
    // budget that the Agent applies to the connect + write + read cycle of each intake request.
    // Apache HttpClient 5's defaults (connect=3min, response=null/infinite) would let a stuck
    // intake hang the connector indefinitely, so we set both legs explicitly.
    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int RESPONSE_TIMEOUT_MS = 10_000;

    private static final Logger log = LoggerFactory.getLogger(DatadogLogsApiWriter.class);
    private final DatadogLogsSinkConnectorConfig config;
    private final Map<String, List<SinkRecord>> batches;
    private final JsonConverter jsonConverter;
    private final CloseableHttpClient httpClient;

    public DatadogLogsApiWriter(DatadogLogsSinkConnectorConfig config) {
        this.config = config;
        this.batches = new HashMap<>();
        this.jsonConverter = new JsonConverter();

        Map<String, String> jsonConverterConfig = new HashMap<>();
        jsonConverterConfig.put("schemas.enable", "false");
        jsonConverterConfig.put("decimal.format", "NUMERIC");
        jsonConverter.configure(jsonConverterConfig, false);

        this.httpClient = buildHttpClient(config);
    }

    private static CloseableHttpClient buildHttpClient(DatadogLogsSinkConnectorConfig config) {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .setResponseTimeout(RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .build();

        HttpClientBuilder builder = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                // Disable automatic decompression: we send gzip, not receive it.
                .disableContentCompression()
                // Disable automatic redirect following to keep behaviour identical to HttpURLConnection defaults.
                .disableRedirectHandling();

        if (config.proxyURL != null && !config.proxyURL.isEmpty()) {
            HttpHost proxy = new HttpHost(config.proxyURL, config.proxyPort);
            builder.setProxy(proxy);
        }

        return builder.build();
    }

    /**
     * Writes records to the Datadog Logs API.
     *
     * @param records to be written from the Source Broker to the Datadog Logs API.
     * @throws IOException may be thrown if the connection to the API fails.
     */
    public void write(Collection<SinkRecord> records) throws IOException {
        for (SinkRecord record : records) {
            if (!batches.containsKey(record.topic())) {
                batches.put(record.topic(), new ArrayList<>(Collections.singletonList(record)));
            } else {
                batches.get(record.topic()).add(record);
            }
            if (batches.get(record.topic()).size() >= config.ddMaxBatchLength) {
                sendBatch(record.topic());
                batches.remove(record.topic());
            }
        }

        // Flush remaining records
        flushBatches();
    }

    private void flushBatches() throws IOException {
        // send any outstanding batches
        for (Map.Entry<String, List<SinkRecord>> entry : batches.entrySet()) {
            sendBatch(entry.getKey());
        }

        batches.clear();
    }

    private void sendBatch(String topic) throws IOException {
        List<String> contentList = formatBatch(topic);
        if (contentList.isEmpty()) {
            log.debug("Nothing to send; Skipping the HTTP request.");
            return;
        }

        URL url = config.getURL();

        for (String content: contentList) {
            sendRequest(content, url);
        }
    }

    private List<String> formatBatch(String topic) {
        List<SinkRecord> sinkRecords = batches.get(topic);

        List<String> serializedBatches = new ArrayList<>();
        StringBuilder currentBatch = new StringBuilder();
        int currentBatchSize = 2; // for '[' and ']' in JSON

        currentBatch.append("[");
        boolean first = true;

        for (SinkRecord record : sinkRecords) {
            if (record == null || record.value() == null) {
                continue;
            }

            JsonElement recordJSON = recordToJSON(record);
            JsonObject message = populateMetadata(topic, recordJSON, record.timestamp(), () -> kafkaHeadersToJsonElement(record));

            String messageString = message.toString();
            int messageSize = utf8ByteLength(messageString);

            // Estimate additional bytes including the extra byte for comma if batch is not empty
            int additionalBytes = messageSize + (!first ? 1 : 0);
            int totalBatchSize = currentBatchSize + additionalBytes;

            // If adding this message would exceed the max batch size
            if (totalBatchSize >= MAXIMUM_BATCH_BYTES) {
                log.debug("Splitting batch because of size limits. Bytes of batch after new message was added: {}", totalBatchSize);

                if (!first) {
                    currentBatch.append("]");
                    serializedBatches.add(currentBatch.toString());
                } else {
                    log.error("Dropping message that exceeds batch size limit ({} bytes, limit {} bytes). Preview: {}",
                            messageSize, MAXIMUM_BATCH_BYTES,
                            messageString.substring(0, Math.min(messageString.length(), 500)));
                    continue;
                }

                // reset (Start new batch)
                currentBatch = new StringBuilder();
                currentBatch.append("[");
                currentBatchSize = 2;
                first = true;
            }

            if (!first) {
                currentBatch.append(",");
            }

            // Add the message to the current batch
            currentBatch.append(messageString);
            currentBatchSize += additionalBytes;
            first = false;
        }

        // Add the last batch if it has messages
        if (!first) {
            currentBatch.append("]");
            serializedBatches.add(currentBatch.toString());
        }

        return serializedBatches;
    }

    /**
     * Returns the exact number of bytes required to encode {@code s} as UTF-8,
     * without allocating a byte array. Uses arithmetic shifts to minimise
     * branches per character (one branch only for surrogate pairs).
     *
     * @see <a href="https://www.rfc-editor.org/rfc/rfc3629">RFC 3629 — UTF-8</a>
     */
    static int utf8ByteLength(String s) {
        int bytes = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char c = s.charAt(i);
            bytes += 1 + ((0x7F - c) >>> 31) + ((0x7FF - c) >>> 31);
            if (Character.isHighSurrogate(c)) { bytes++; i++; }
        }
        return bytes;
    }

    private JsonElement kafkaHeadersToJsonElement(SinkRecord sinkRecord) {
        Map<String, Object> headerMap = stream(sinkRecord.headers().spliterator(), false)
                .collect(toMap(Header::key, Header::value));

        Gson gson = new Gson();

        String jsonString = gson.toJson(headerMap);

        return gson.fromJson(jsonString, JsonElement.class);
    }

    private JsonElement recordToJSON(SinkRecord record) {
        byte[] rawJSONPayload = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        String jsonPayload = new String(rawJSONPayload, StandardCharsets.UTF_8);
        return new Gson().fromJson(jsonPayload, JsonElement.class);
    }

    private JsonObject populateMetadata(String topic, JsonElement message, Long timestamp, Supplier<JsonElement> kafkaHeaders) {
        JsonObject content = new JsonObject();
        String tags = "topic:" + topic;
        content.add("message", message);
        content.add("ddsource", new JsonPrimitive(config.ddSource));
        if (config.addPublishedDate && timestamp != null) {
            content.add("published_date", new JsonPrimitive(timestamp));
        }

        if (config.parseRecordHeaders) {
            content.add("kafkaheaders", kafkaHeaders.get());
        }

        if (config.ddTags != null) {
            tags += "," + config.ddTags;
        }
        content.add("ddtags", new JsonPrimitive(tags));

        if (config.ddHostname != null) {
            content.add("hostname", new JsonPrimitive(config.ddHostname));
        }

        if (config.ddService != null) {
            content.add("service", new JsonPrimitive(config.ddService));
        }

        return content;
    }

    private void sendRequest(String requestContent, URL url) throws IOException {
        byte[] compressedPayload = compress(requestContent);

        String urlString = url.toString();
        HttpPost request = new HttpPost(urlString);

        request.setHeader("Content-Type", "application/json");
        request.setHeader("Content-Encoding", "gzip");
        request.setHeader("DD-API-KEY", config.ddApiKey);
        request.setHeader("DD-EVP-ORIGIN", Project.getName());
        request.setHeader("DD-EVP-ORIGIN-VERSION", Project.getVersion());
        request.setHeader("User-Agent", Project.getName() + "/" + Project.getVersion());

        request.setEntity(new ByteArrayEntity(compressedPayload, ContentType.APPLICATION_JSON));

        log.trace("Submitting HTTP request to {} with body {}", urlString, requestContent);

        httpClient.execute(request, response -> {
            int status = response.getCode();
            if (!isSuccessfulHttpStatus(status)) {
                String error = "";
                if (response.getEntity() != null) {
                    try {
                        error = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    } catch (IOException | ParseException ignored) {
                        // best-effort error body read
                    }
                }
                log.error("Http request failed with status: {}", status);
                throw new IOException("HTTP Response code: " + status
                        + ", " + response.getReasonPhrase() + ", " + error);
            }
            String body = "";
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try {
                    body = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                } catch (ParseException ignored) {
                    // best-effort response body read
                }
            }
            log.trace("Received HTTP response {} {} with body {}", status, response.getReasonPhrase(), body);
            return null;
        });

        log.trace("HTTP request submitted");
    }

    /**
     * Check if the HTTP status code indicates success (2xx range)
     *
     * @param statusCode HTTP status code to check
     * @return true if status code is in the 2xx range (200-299)
     */
    private boolean isSuccessfulHttpStatus(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private byte[] compress(String str) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream(str.length());
        GZIPOutputStream gos = new GZIPOutputStream(os);
        gos.write(str.getBytes());
        os.close();
        gos.close();
        return os.toByteArray();
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
