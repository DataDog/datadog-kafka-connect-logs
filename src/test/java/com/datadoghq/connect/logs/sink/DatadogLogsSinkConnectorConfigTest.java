/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DatadogLogsSinkConnectorConfigTest {
    private Map<String, String> props;

    @Test
    public void constructor_givenEmptyAPIKey_shouldThrowException() {
        props = new HashMap<>();
        assertThrows(ConfigException.class, () -> {
            new DatadogLogsSinkConnectorConfig(props);
        });
    }

    @Test
    public void getTags_givenValidList_shouldCreateString() {
        props = new HashMap<>();
        props.put(DatadogLogsSinkConnectorConfig.DD_API_KEY, "123");
        props.put(DatadogLogsSinkConnectorConfig.DD_TAGS, "test1,test2,test3");
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(props);

        assertEquals("test1,test2,test3", config.ddTags);
    }

    @Test
    public void getUrl_givenValidProps_shouldReturnString() {
        props = new HashMap<>();
        props.put(DatadogLogsSinkConnectorConfig.DD_API_KEY, "123");
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(props);
        
        props.put(DatadogLogsSinkConnectorConfig.DD_URL, "example.com");
        DatadogLogsSinkConnectorConfig customConfig = new DatadogLogsSinkConnectorConfig(props);


        assertEquals(DatadogLogsSinkConnectorConfig.DEFAULT_DD_URL, config.ddUrl);
        assertEquals("example.com", customConfig.ddUrl);
    }
}
