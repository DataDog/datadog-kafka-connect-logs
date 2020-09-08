/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.List;
import java.util.Map;

public class DatadogLogsSinkConnectorConfig extends AbstractConfig {

    public static final String DD_TAGS = "datadog.tags";
    public static final String DD_SERVICE = "datadog.service";
    public static final String DD_HOSTNAME = "datadog.hostname";
    public static final String DD_API_KEY = "datadog.api_key";
    public static final String PROXY_URL = "datadog.proxy.url";
    public static final String PROXY_PORT = "datadog.proxy.port";
    public static final String MAX_RETRIES = "datadog.retry.max";
    public static final String RETRY_BACKOFF_MS = "datadog.retry.backoff_ms";

    // Respect limit documented at https://docs.datadoghq.com/api/?lang=bash#logs
    public final Integer ddMaxBatchLength;
    public final String ddSource = "kafka-connect";

    // Only for testing
    public final Boolean useSSL;
    public final String url;

    public final String ddTags;
    public final String ddService;
    public final String ddHostname;
    public final String ddApiKey;
    public final String proxyURL;
    public final Integer proxyPort;
    public final Integer retryMax;
    public final Integer retryBackoffMs;

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public DatadogLogsSinkConnectorConfig(Map<String, String> props) {
        super(baseConfigDef(), props);
        ddTags = getTags(DD_TAGS);
        ddService = getString(DD_SERVICE);
        ddHostname = getString(DD_HOSTNAME);
        ddApiKey = getPasswordValue(DD_API_KEY);
        proxyURL = getString(PROXY_URL);
        proxyPort = getInt(PROXY_PORT);
        retryMax = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        useSSL = true;
        url = "http-intake.logs.datad0g.com:443";
        ddMaxBatchLength = 500;
        validateConfig();
    }

    public DatadogLogsSinkConnectorConfig(Boolean useSSL, String url, Integer ddMaxBatchLength, Map<String, String> props) {
        super(baseConfigDef(), props);
        ddTags = getTags(DD_TAGS);
        ddService = getString(DD_SERVICE);
        ddHostname = getString(DD_HOSTNAME);
        ddApiKey = getPasswordValue(DD_API_KEY);
        proxyURL = getString(PROXY_URL);
        proxyPort = getInt(PROXY_PORT);
        retryMax = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        this.useSSL = useSSL;
        this.url = url;
        this.ddMaxBatchLength = ddMaxBatchLength;
        validateConfig();
    }

    private void validateConfig() {
        if (getPasswordValue(DD_API_KEY) == null) {
            throw new ConfigException("API Key must not be empty.");
        }
    }

    private static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addMetadataConfigs(configDef);
        addProxyConfigs(configDef);
        addRetryConfigs(configDef);
        return configDef;
    }

    private static void addMetadataConfigs(ConfigDef configDef) {
        int orderInGroup = 0;
        final String group = "Datadog Metadata";

        configDef.define(
                DD_API_KEY,
                Type.PASSWORD,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                "The API key of your Datadog platform.",
                group,
                ++orderInGroup,
                Width.LONG,
                "API Key"
        ).define(
                DD_TAGS,
                Type.LIST,
                null,
                Importance.MEDIUM,
                "Tags associated with your logs in a comma separated tag:value format.",
                group,
                ++orderInGroup,
                Width.LONG,
                "Tags Metadata"
        ).define(
                DD_SERVICE,
                Type.STRING,
                null,
                Importance.MEDIUM,
                "The name of the application or service generating the log events.",
                group,
                ++orderInGroup,
                Width.LONG,
                "Service Metadata"
        ).define(
                DD_HOSTNAME,
                Type.STRING,
                null,
                Importance.MEDIUM,
                "The name of the originating host of the log.",
                group,
                ++orderInGroup,
                Width.LONG,
                "Hostname Metadata"
        );
    }

    private static void addProxyConfigs(ConfigDef configDef) {
        int orderInGroup = 0;
        final String group = "Datadog Proxy";

        configDef.define(
                PROXY_URL,
                Type.STRING,
                null,
                Importance.LOW,
                "Proxy endpoint when logs are not directly forwarded to Datadog.",
                group,
                ++orderInGroup,
                Width.LONG,
                "Proxy URL"
        ).define(
                PROXY_PORT,
                Type.INT,
                null,
                Importance.LOW,
                "Proxy port when logs are not directly forwarded to Datadog.",
                group,
                ++orderInGroup,
                Width.LONG,
                "Proxy Port"
        );
    }

    private static void addRetryConfigs(ConfigDef configDef) {
        int orderInGroup = 0;
        final String group = "Datadog Retry";

        configDef.define(
                MAX_RETRIES,
                Type.INT,
                5,
                Importance.LOW,
                "The number of retries before the output plugin stops.",
                group,
                ++orderInGroup,
                Width.LONG,
                "Max Retries"
        ).define(
                RETRY_BACKOFF_MS,
                Type.INT,
                3000,
                Importance.LOW,
                "The time in milliseconds to wait following an error before a retry attempt is made.",
                group,
                ++orderInGroup,
                Width.LONG,
                "Retry Backoff (millis)"
        );
    }

    private String getPasswordValue(String key) {
        Password password = getPassword(key);
        if (password != null) {
            return password.value();
        }

        return null;
    }

    private String getTags(String key) {
        List<String> tags = getList(key);
        if (tags != null) {
            return String.join(",", tags);
        }

        return null;
    }
}