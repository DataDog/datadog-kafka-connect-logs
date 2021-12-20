package com.datadoghq.connect.logs.util;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.metrics.stats.Avg;

import java.util.ArrayList;
import java.util.Arrays;

public class Telemetry {
    Sensor batchesSent;
    Sensor batchSize;

    public Telemetry() {
        MetricConfig metricConfig = new MetricConfig();
        Metrics metrics = new Metrics(metricConfig);
        batchesSent = metrics.sensor("batches-sent");
        batchSize = metrics.sensor("batch-size");

        MetricName batchesSentAvg = metrics.metricName(
                "batches-sent-avg",
                "kafka-connect-logs-metrics",
                "Average number of messages sent"
        );
        batchesSent.add(batchesSentAvg, new Avg());

        MetricName batchSizeAvg = metrics.metricName(
                "batch-size-avg",
                "kafka-connect-logs-metrics",
                "Average batch size"
        );
        batchSize.add(batchSizeAvg, new Avg());

    }

    public void recordBatchesSent() {
        batchesSent.record();
    }

    public void recordBatchSize(double size) {
        batchSize.record(size);
    }
}
