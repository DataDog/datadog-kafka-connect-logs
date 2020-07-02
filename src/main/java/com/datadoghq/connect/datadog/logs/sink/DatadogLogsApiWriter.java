package com.datadoghq.connect.datadog.logs.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DatadogLogsApiWriter {
    private final DatadogLogsSinkConnectorConfig config;
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsApiWriter.class);

    public DatadogLogsApiWriter(DatadogLogsSinkConnectorConfig config) {
        this.config = config;
    }

    public void write(Collection<SinkRecord> records) {
        List<String> batches = batchRecords(records, config.DD_MAX_BATCH_LENGTH, config.DD_MAX_BATCH_SIZE);
        // TODO: For each batch process payload
        // TODO: In processing, compress if necessary and then send similar to the HTTP Sink connector
    }

    private List<String> batchRecords(Collection<SinkRecord> records, int ddMaxBatchLength, int ddMaxBatchSize) {
        List<String> batches = new ArrayList<>();
        List<String> currentBatches = new ArrayList<>();

        // TODO: Batch similar to logstash plugin

    }


}
