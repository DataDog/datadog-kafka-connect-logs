package com.datadoghq.connect.datadog.logs.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class DatadogLogsApiWriter {
    private final DatadogLogsSinkConnectorConfig config;
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsApiWriter.class);
    private final Map<String, List<SinkRecord>> batches = new HashMap<>();

    public DatadogLogsApiWriter(DatadogLogsSinkConnectorConfig config) {
        this.config = config;
    }

    public void write(Collection<SinkRecord> records) throws IOException {
        for (SinkRecord record : records) {

            // Key to identify the batch by record topic and record key
            String keyPattern = record.topic() + ":" + (record.key() == null ? "" : record.key().toString());

            if (!batches.containsKey(keyPattern)) {
                batches.put(keyPattern, new ArrayList<>(Collections.singletonList(record)));
            } else {
                batches.get(keyPattern).add(record);
            }

            if (batches.get(keyPattern).size() >= config.ddMaxBatchLength) {
                sendBatch(keyPattern);
            }
        }

        flushBatches();
    }

    private void flushBatches() throws IOException {
        // Send remaining batches
        for (Map.Entry<String, List<SinkRecord>> entry: batches.entrySet()) {
            sendBatch(entry.getKey());
        }
    }

    private void sendBatch(String keyPattern) throws IOException {
        List<SinkRecord> records = batches.get(keyPattern);

        StringBuilder builder = new StringBuilder();
        int batchIndex = 0;
        for (SinkRecord record : records) {
            batchIndex++;

            if (record == null) {
                continue;
            }

            if (record.value() == null) {
                continue;
            }

            builder.append(record.value().toString());
            if (batchIndex < records.size()) {
                builder.append(",");
            }
        }

        if (builder.length() == 0) {
            log.debug("Nothing to send; Skipping the HTTP request.");
            return;
        }

        URL url = new URL("http://" + config.hostname + ":" + config.port.toString() + "/v1/input/" + config.apiKey);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        String requestContent = builder.toString();

        if (config.useCompression) {
            con.setRequestProperty("Content-Encoding", "gzip");
            requestContent = compress(requestContent);
        }

        OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream(), StandardCharsets.UTF_8);
        writer.write(requestContent);
        writer.close();

        //clear batch
        batches.remove(keyPattern);

        log.debug("Submitted payload: " + builder.toString() + ", url:" + url);

        // get response
        int status = con.getResponseCode();
        if (Response.Status.Family.familyOf(status) != Response.Status.Family.SUCCESSFUL) {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getErrorStream()));
            String inputLine;
            StringBuilder error = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                error.append(inputLine);
            }
            in.close();
            throw new IOException("HTTP Response code: " + status
                    + ", " + con.getResponseMessage() + ", " + error
                    + ", Submitted payload: " + builder.toString()
                    + ", url:" + url);
        }
        log.debug(", response code: " + status + ", " + con.getResponseMessage());

        // write the response to the log
        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuilder content = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }

        log.debug("Response content: " + content);
        in.close();
        con.disconnect();
    }

    private static String compress(String str) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream(str.length());
        GZIPOutputStream gos = new GZIPOutputStream(os);
        gos.write(str.getBytes());
        os.close();
        gos.close();
        return Base64.getEncoder().encodeToString(os.toByteArray());
    }

}
