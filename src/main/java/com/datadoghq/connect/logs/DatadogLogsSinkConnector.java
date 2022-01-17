/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs;

import com.datadoghq.connect.logs.sink.DatadogLogsSinkConnectorConfig;
import com.datadoghq.connect.logs.sink.DatadogLogsSinkTask;
import com.datadoghq.connect.logs.util.Project;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatadogLogsSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(DatadogLogsSinkConnector.class);
    private Map<String, String> configProps;

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>(configProps);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Datadog Logs Sink Connector.");
        configProps = props;
    }

    @Override
    public void stop() {
        log.info("Stopping Datadog Logs Sink Connector.");
    }

    @Override
    public ConfigDef config() {
        return DatadogLogsSinkConnectorConfig.CONFIG_DEF;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DatadogLogsSinkTask.class;
    }

    @Override
    public String version() {
        return Project.getVersion();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        log.info("Validating Datadog Logs Sink Connector config.");
        return super.validate(connectorConfigs);
    }
}
