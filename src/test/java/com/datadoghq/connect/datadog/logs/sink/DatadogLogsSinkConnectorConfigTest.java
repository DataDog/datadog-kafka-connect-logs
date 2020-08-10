package com.datadoghq.connect.datadog.logs.sink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

public class DatadogLogsSinkConnectorConfigTest {

    @Test
    public void constructor_givenEmptyAPIKey_shouldThrowException() {
        Map<String, String> props = new HashMap<>();
        assertThrows(ConfigException.class, () -> {
            new DatadogLogsSinkConnectorConfig(props);
        });
    }

    @Test
    public void getTags_givenValidList_shouldCreateString() {
        Map<String, String> props = new HashMap<>();
        props.put(DatadogLogsSinkConnectorConfig.DD_API_KEY, "123");
        props.put(DatadogLogsSinkConnectorConfig.DD_TAGS, "test1,test2,test3");
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(props);

        assertEquals("test1,test2,test3", config.ddTags);
    }
}
