package com.datadoghq.connect.datadog.logs.sink;

import com.datadoghq.connect.datadog.logs.util.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import org.slf4j.MDC;

//TODO: Implement, with DatadogLogsApiWriter!

public class DatadogLogsSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsSinkTask.class);

    DatadogLogsSinkConnectorConfig config;
    DatadogLogsApiWriter writer;
    int remainingTries;

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

    }

    @Override
    public void stop() {
        log.info("Stopping task for {}", context.configs().get("name"));
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
