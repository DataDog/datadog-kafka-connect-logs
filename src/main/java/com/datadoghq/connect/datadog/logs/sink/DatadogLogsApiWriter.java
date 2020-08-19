package com.datadoghq.connect.datadog.logs.sink;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.HttpsURLConnection;
import javax.ws.rs.core.Response;

public class DatadogLogsApiWriter {
    private final DatadogLogsSinkConnectorConfig config;
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsApiWriter.class);
    private final List<SinkRecord> batch = new ArrayList<>();

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
            if (batch.size() >= config.ddMaxBatchLength) {
                sendBatch();
            }

            batch.add(record);
        }

        // Flush remaining records
        sendBatch();
    }

    private void sendBatch() throws IOException {
        JsonArray message = formatBatch();
        if (message.size() == 0) {
            log.debug("Nothing to send; Skipping the HTTP request.");
            return;
        }

        JsonObject content = populateMetadata(message);
        String protocol = config.useSSL ? "https://" : "http://";

        URL url = new URL(
                protocol
                        + config.url
                        + ":"
                        + config.port.toString()
                        + "/v1/input/"
                        + config.ddApiKey
        );
        if (config.useSSL) {
            sendHTTPSRequest(content, url);
        } else {
            sendHTTPRequest(content, url);
        }
    }

    private JsonObject populateMetadata(JsonArray message) {
        JsonObject content = new JsonObject();
        content.add("message", message);
        content.add("ddsource", new JsonPrimitive(config.ddSource));

        if (config.ddTags != null) {
            content.add("ddtags", new JsonPrimitive(config.ddTags));
        }

        if (config.ddHostname != null) {
            content.add("hostname", new JsonPrimitive(config.ddHostname));
        }

        if (config.ddService != null) {
            content.add("service", new JsonPrimitive(config.ddService));
        }

        return content;
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

    private void sendHTTPSRequest(JsonObject content, URL url) throws IOException {
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
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

        batch.clear();

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

    private void sendHTTPRequest(JsonObject content, URL url) throws IOException {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
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


        batch.clear();

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

    private JsonArray formatBatch() {
        JsonArray batchRecords = new JsonArray();

        for (SinkRecord record : batch) {
            if (record == null) {
                continue;
            }

            if (record.value() == null) {
                continue;
            }

            JsonPrimitive recordJSON = recordToJSON(record);
            batchRecords.add(recordJSON);
        }

        return batchRecords;
    }

    private JsonPrimitive recordToJSON(SinkRecord record) {
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);

        byte[] rawJSONPayload = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        String jsonPayload = new String(rawJSONPayload, StandardCharsets.UTF_8);
        return new Gson().fromJson(jsonPayload, JsonPrimitive.class);
    }

    private byte[] compress(String str) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream(str.length());
        GZIPOutputStream gos = new GZIPOutputStream(os);
        gos.write(str.getBytes());
        os.close();
        gos.close();
        return os.toByteArray();
    }
}
