/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

import com.datadoghq.connect.logs.util.RetryUtil;
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
import java.util.concurrent.TimeUnit;

public class DatadogLogsSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsSinkTask.class);

    DatadogLogsSinkConnectorConfig config;
    DatadogLogsApiWriter writer;
    int remainingRetries;

    // Only for testing
    public Integer retryOverride;

    @Override
    public void start(Map<String, String> settings) {
        log.info("Starting Sink Task.");
        config = new DatadogLogsSinkConnectorConfig(settings);
        initWriter();
        logMaxRetryBackoffMs(config.retryMax, config.retryBackoffMs);
        remainingRetries = config.retryMax;
    }

    private void logMaxRetryBackoffMs(Integer retryMax, Integer retryBackoffMs) {
        long maxRetryBackoffMs = RetryUtil.computeRetryWaitTimeInMillis(retryMax, retryBackoffMs);
        if (maxRetryBackoffMs > RetryUtil.MAX_RETRY_TIME_MS) {
            log.warn("This connector uses exponential backoff with jitter for retries, and using '{}={}' and '{}={}' " +
                            "results in an impractical but possible maximum backoff time greater than {} hours.",
                    DatadogLogsSinkConnectorConfig.MAX_RETRIES, retryMax,
                    DatadogLogsSinkConnectorConfig.RETRY_BACKOFF_MS, retryBackoffMs,
                    TimeUnit.MILLISECONDS.toHours(maxRetryBackoffMs));
        }
    }

    protected void initWriter() {
        writer = new DatadogLogsApiWriter(config);
        this.retryOverride = 0;
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
                long sleepTimeMs = RetryUtil.computeRandomRetryWaitTimeInMillis(remainingRetries, config.retryBackoffMs);
                remainingRetries--;

                if (retryOverride > 0) {
                    context.timeout(retryOverride);
                } else {
                    context.timeout(sleepTimeMs);
                }

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
        log.info("Stopping task for {}", context.configs().get("name"));
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }
}
