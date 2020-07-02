package com.datadoghq.connect.datadog.logs.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class DatadogLogsSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsSinkTask.class);

    DatadogLogsSinkConnectorConfig config;
    DatadogLogsApiWriter writer;
    int remainingRetries;

    @Override
    public void start(Map<String, String> settings) {
        log.info("Starting Sink Task.");
        config = new DatadogLogsSinkConnectorConfig(settings);
        initWriter();
    }

    private void initWriter() {
        writer = new DatadogLogsApiWriter(config);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.trace(
                "Received {} records. First record Kafka coordinates:({}-{}-{}). Writing them to the API...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );

        try {
            writer.write(records);
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
                remainingRetries--;
                context.timeout(config.retryBackoffMs);
                throw new RetriableException(e);
            }
        }

        remainingRetries = config.maxRetries;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // no-op
    }

    @Override
    public void stop() {
        log.info("Stopping task for {}", context.configs().get("name"));
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }
}
