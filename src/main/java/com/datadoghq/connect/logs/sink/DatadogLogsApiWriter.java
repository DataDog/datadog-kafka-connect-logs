/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

import com.datadoghq.connect.logs.util.Project;
import com.google.gson.*;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import javax.naming.SizeLimitExceededException;
import javax.ws.rs.core.Response;

public class DatadogLogsApiWriter {
    public static final int ONE_MEGABYTE = 1000000;
    public static final int MAXIMUM_BATCH_MEGABYTES = 3;
    public static final int MINIMUM_LIST_LENGTH = 0;
    public static final int MINIMUM_STRING_LENGTH = 0;
    public static final int TRUNCATE_DIVIDER = 2;
    private final DatadogLogsSinkConnectorConfig config;
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsApiWriter.class);
    private final Map<String, List<SinkRecord>> batches;
    private final JsonConverter jsonConverter;

    public DatadogLogsApiWriter(DatadogLogsSinkConnectorConfig config) {
        this.config = config;
        this.batches = new HashMap<>();
        this.jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    /**
     * Writes records to the Datadog Logs API.
     * @param records to be written from the Source Broker to the Datadog Logs API.
     * @throws IOException may be thrown if the connection to the API fails.
     */
    public void write(Collection<SinkRecord> records) throws IOException {
        for (SinkRecord record : records) {
            if (!batches.containsKey(record.topic())) {
                batches.put(record.topic(), new ArrayList<> (Collections.singletonList(record)));
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
        for(Map.Entry<String,List<SinkRecord>> entry: batches.entrySet()) {
            sendBatch(entry.getKey());
        }

        batches.clear();
    }

    private void sendBatch(String topic) throws IOException {
        List<JsonArray> contentList = formatBatch(topic);
        if (contentList.size() == 0) {
            log.debug("Nothing to send; Skipping the HTTP request.");
            return;
        }

        URL url = config.getURL();

        for (JsonArray content: contentList) {
            sendRequest(content, url);
        }
    }

    private List<JsonArray> formatBatch(String topic) {
        List<SinkRecord> sinkRecords = batches.get(topic);
        JsonArray batchRecords = new JsonArray();
        List<JsonArray> batchRecordsList = new ArrayList<>();

        batchRecordsList.add(batchRecords);
        int currentBatchListIndex = 0;

        for (SinkRecord record : sinkRecords) {
            if (record == null) {
                continue;
            }

            if (record.value() == null) {
                continue;
            }

            JsonElement recordJSON = recordToJSON(record);
            JsonObject message = populateMetadata(topic, recordJSON);
            JsonArray currentBatchRecords = batchRecordsList.get(currentBatchListIndex);

            try {
                if (hasBatchOverflowed(message, currentBatchRecords)) {
                    JsonArray emptyBatch = new JsonArray();

                    emptyBatch.add(message);
                    batchRecordsList.add(currentBatchListIndex++, emptyBatch);
                } else {
                    currentBatchRecords.add(message);
                }

            } catch (SizeLimitExceededException ex) {
                log.error("Single message exceeds size limit.", ex);
            }
        }
        return batchRecordsList;
    }

    private boolean hasBatchOverflowed(JsonObject jsonRecord, JsonArray
            batchRecords) throws SizeLimitExceededException {
        String jsonRecordString = jsonRecord.toString();
        UUID id = UUID.randomUUID();

        batchRecords.add(jsonRecord);
        String batchRecordsString = batchRecords.toString();
        final byte[] batchRecordsStringBytes = batchRecordsString.getBytes(StandardCharsets.UTF_8);
        batchRecords.remove(jsonRecord);

        if (batchRecordsStringBytes.length / ONE_MEGABYTE >= MAXIMUM_BATCH_MEGABYTES) {
            log.warn("Splitting batch because of size limits. Bytes of batch after new message was added: " + batchRecordsStringBytes.length + " id: " + id);
            final byte[] jsonRecordStringBytes = jsonRecordString.getBytes(StandardCharsets.UTF_8);

            if (batchRecords.size() == MINIMUM_LIST_LENGTH || jsonRecordStringBytes.length / ONE_MEGABYTE >= MAXIMUM_BATCH_MEGABYTES) {
                throw new SizeLimitExceededException(String.format("Single message exceeds JSON size limit. " +
                        "Truncated message: %s, id: %s", jsonRecordString.substring(MINIMUM_STRING_LENGTH, jsonRecordString.length() / TRUNCATE_DIVIDER), id));
            }
            return true;
        }
        return false;
    }

    private JsonElement recordToJSON(SinkRecord record) {
        byte[] rawJSONPayload = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        String jsonPayload = new String(rawJSONPayload, StandardCharsets.UTF_8);
        return new Gson().fromJson(jsonPayload, JsonElement.class);
    }

    private JsonObject populateMetadata(String topic, JsonElement message) {
        JsonObject content = new JsonObject();
        String tags = "topic:" + topic;
        content.add("message", message);
        content.add("ddsource", new JsonPrimitive(config.ddSource));

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

    private void sendRequest(JsonArray content, URL url) throws IOException {
        String requestContent = content.toString();
        byte[] compressedPayload = compress(requestContent);

        HttpURLConnection con;
        if (config.proxyURL != null) {
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(config.proxyURL, config.proxyPort));
            con = (HttpURLConnection) url.openConnection(proxy);
        } else {
            con = (HttpURLConnection) url.openConnection();
        }
        con.setDoOutput(true);
        con.setRequestMethod("POST");
        setRequestProperties(con);

        DataOutputStream output = new DataOutputStream(con.getOutputStream());
        output.write(compressedPayload);
        output.close();
        log.debug("Submitted payload: " + requestContent);

        // get response
        int status = con.getResponseCode();
        if (Response.Status.Family.familyOf(status) != Response.Status.Family.SUCCESSFUL) {
            InputStream stream = con.getErrorStream();
            UUID payloadErrorId = UUID.randomUUID();
            String error = "";
            if (stream != null ) {
                error = getOutput(stream);
            }

            con.disconnect();
            log.error(String.format("Data content for error id: %s, content: %s", payloadErrorId, con.getContent()));
            throw new IOException("HTTP Response code: " + status
                    + ", " + con.getResponseMessage() + ", " + error
                    + " Error Id: " + payloadErrorId);
        }

        log.debug("Response code: " + status + ", " + con.getResponseMessage());

        // write the response to the log
        String response = getOutput(con.getInputStream());

        log.debug("Response content: " + response);
        con.disconnect();
    }

    private void setRequestProperties(HttpURLConnection con) {
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Content-Encoding", "gzip");
        con.setRequestProperty("DD-API-KEY", config.ddApiKey);
        con.setRequestProperty("DD-EVP-ORIGIN", Project.getName());
        con.setRequestProperty("DD-EVP-ORIGIN-VERSION", Project.getVersion());
    }

    private byte[] compress(String str) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream(str.length());
        GZIPOutputStream gos = new GZIPOutputStream(os);
        gos.write(str.getBytes());
        os.close();
        gos.close();
        return os.toByteArray();
    }

    private String getOutput(InputStream input) throws IOException {
        ByteArrayOutputStream errorOutput = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = input.read(buffer)) != -1) {
            errorOutput.write(buffer, 0, length);
        }

        return errorOutput.toString(StandardCharsets.UTF_8.name());
    }

}
