/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DatadogLogsSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsSinkTask.class);
    private static final long MAX_RETRY_TIME_MS = TimeUnit.MINUTES.toMillis(10);

    DatadogLogsSinkConnectorConfig config;
    DatadogLogsApiWriter writer;
    HttpURLConnection con;
    int remainingRetries;

    @Override
    public void start(Map<String, String> settings) {
        log.info("Starting Sink Task.");
        config = new DatadogLogsSinkConnectorConfig(settings);
        initWriter();
        remainingRetries = config.retryMax;

        try {
            con = createNewHTTPConnection(config);
        } catch (IOException e) {
            log.error("Error creating new HTTP connection {}", e.toString());
            throw new ConnectException(e);
        }
    }

    protected void initWriter() {
        writer = new DatadogLogsApiWriter(config);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.debug(
                "Received {} records. First record Kafka coordinates:({}-{}-{}). Writing them to the API...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );

        try {
            writer.write(records, con);
        } catch (Exception e) {
            log.warn(
                    "Write of {} records failed, remaining retries: {}",
                    records.size(),
                    remainingRetries,
                    e
            );

            if (remainingRetries == 0) {
                throw new ConnectException(e);
            } else {
                initWriter();
                long sleepTimeMs = computeRetryWaitMs(
                        config.retryMax-remainingRetries,
                        config.retryBackoffMs
                );
                remainingRetries--;
                context.timeout(sleepTimeMs);
                throw new RetriableException(e);
            }
        }

        remainingRetries = config.retryMax;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.debug("Flushing data to Datadog with the following offsets: {}", offsets);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        log.debug("Closing the task for topic partitions: {}", partitions);
        con.disconnect();
    }

    @Override
    public void stop() {
        log.info("Stopping task for {}", context.configs().get("name"));
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    protected long computeRetryWaitMs(int retryAttempts, long retryBackoffMs) {
        if (retryAttempts > 0 && retryAttempts <= 32) {
            long waitDuration = retryBackoffMs << retryAttempts;
            waitDuration = Math.min(waitDuration, MAX_RETRY_TIME_MS);
            return ThreadLocalRandom.current().nextLong(0, waitDuration);
        }

        return retryBackoffMs;
    }

    protected HttpURLConnection createNewHTTPConnection(DatadogLogsSinkConnectorConfig config) throws IOException {
        String protocol = config.useSSL ? "https://" : "http://";

        URL url = new URL(
                protocol
                        + config.url
                        + "/v1/input/"
                        + config.ddApiKey
        );

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

        return con;
    }
}
