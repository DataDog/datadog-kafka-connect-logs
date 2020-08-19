package com.datadoghq.connect.datadog.logs.sink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DatadogLogsSinkConnectorConfigTest {
    private Map<String, String> props;

    @Before
    public void setUp() {
        props = new HashMap<>();
    }

    @After
    public void tearDown() {
        props = new HashMap<>();
    }

    @Test
    public void constructor_givenEmptyAPIKey_shouldThrowException() {
        assertThrows(ConfigException.class, () -> {
            new DatadogLogsSinkConnectorConfig(props);
        });
    }

    @Test
    public void getTags_givenValidList_shouldCreateString() {
        props.put(DatadogLogsSinkConnectorConfig.DD_API_KEY, "123");
        props.put(DatadogLogsSinkConnectorConfig.DD_TAGS, "test1,test2,test3");
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(props);

        assertEquals("test1,test2,test3", config.ddTags);
    }
}
