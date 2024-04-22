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

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DatadogLogsSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsSinkTask.class);
    private static final long MAX_RETRY_TIME_MS = TimeUnit.MINUTES.toMillis(10);
    private static final long threadId = Thread.currentThread().getId();

    DatadogLogsSinkConnectorConfig config;
    DatadogLogsApiWriter writer;
    int remainingRetries;

    @Override
    public void start(Map<String, String> settings) {
        config = new DatadogLogsSinkConnectorConfig(settings);
        log.info("Starting task with config={}", config);
        initWriter();
        remainingRetries = config.retryMax;
    }

    protected void initWriter() {
        writer = new DatadogLogsApiWriter(config);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        Instant start = Instant.now();

        final int recordsCount = records.size();
        log.debug("Received {} records", recordsCount);

        if (!records.isEmpty()) {
            final SinkRecord first = records.iterator().next();
            log.debug(
                    "First record Kafka coordinates: ({}-{}-{})",
                    first.topic(), first.kafkaPartition(), first.kafkaOffset()
            );
        }

        try {
            writer.write(records);
            log.debug(
                    "Wrote {} records in {}ms",
                    recordsCount, Duration.between(start, Instant.now()).toMillis()
            );
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
                        config.retryMax - remainingRetries,
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
    }

    @Override
    public void stop() {
        log.info("Stopping task with config={}", config);
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
}
