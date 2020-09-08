/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

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
import javax.ws.rs.core.Response;

public class DatadogLogsApiWriter {
    private final DatadogLogsSinkConnectorConfig config;
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsApiWriter.class);
    private Map<String, List<SinkRecord>> batches = new HashMap<>();

    public DatadogLogsApiWriter(DatadogLogsSinkConnectorConfig config) {
        this.config = config;
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
        JsonArray content = formatBatch(topic);
        if (content.size() == 0) {
            log.debug("Nothing to send; Skipping the HTTP request.");
            return;
        }

        String protocol = config.useSSL ? "https://" : "http://";

        URL url = new URL(
                protocol
                        + config.url
                        + "/v1/input/"
                        + config.ddApiKey
        );

        sendRequest(content, url);
    }

    private JsonArray formatBatch(String topic) {
        List<SinkRecord> sinkRecords = batches.get(topic);
        JsonArray batchRecords = new JsonArray();

        for (SinkRecord record : sinkRecords) {
            if (record == null) {
                continue;
            }

            if (record.value() == null) {
                continue;
            }

            JsonElement recordJSON = recordToJSON(record);
            JsonObject message = populateMetadata(topic, recordJSON);
            batchRecords.add(message);
        }

        return batchRecords;
    }

    private JsonElement recordToJSON(SinkRecord record) {
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);

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
        HttpURLConnection con;
        if (config.proxyURL != null) {
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(config.proxyURL, config.proxyPort));
            con = (HttpURLConnection) url.openConnection(proxy);
        } else {
            con = (HttpURLConnection) url.openConnection();
        }
        con.setDoOutput(true);
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Content-Encoding", "gzip");
        String requestContent = content.toString();
        byte[] compressedPayload = compress(requestContent);

        DataOutputStream output = new DataOutputStream(con.getOutputStream());
        output.write(compressedPayload);
        output.close();
        log.debug("Submitted payload: " + requestContent);

        // get response
        int status = con.getResponseCode();
        if (Response.Status.Family.familyOf(status) != Response.Status.Family.SUCCESSFUL) {
            String error = getOutput(con.getErrorStream());
            con.disconnect();
            throw new IOException("HTTP Response code: " + status
                    + ", " + con.getResponseMessage() + ", " + error
                    + ", Submitted payload: " + content);
        }

        log.debug("Response code: " + status + ", " + con.getResponseMessage());

        // write the response to the log
        String response = getOutput(con.getInputStream());

        log.debug("Response content: " + response);
        con.disconnect();
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
