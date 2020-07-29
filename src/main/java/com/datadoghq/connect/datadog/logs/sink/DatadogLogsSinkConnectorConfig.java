package com.datadoghq.connect.datadog.logs.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.NonEmptyStringWithoutControlChars;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.List;
import java.util.Map;

public class DatadogLogsSinkConnectorConfig extends AbstractConfig {

    private static final String DD_TAGS = "tags";
    private static final String DD_SERVICE = "service";
    private static final String DD_HOSTNAME = "hostname";
    private static final String DD_API_KEY = "api_key";
    private static final String URL = "proxy.url";
    private static final String PORT = "proxy.port";
    private static final String RETRY_MAX = "retry.max";
    private static final String RETRY_BACKOFF_MS = "retry.backoff_ms";

    // Respect limit documented at https://docs.datadoghq.com/api/?lang=bash#logs
    public final Integer ddMaxBatchLength = 500;
    public final String ddSource = "kafka-connect";

    public final String ddTags;
    public final String ddService;
    public final String ddHostname;
    public final String ddApiKey;
    public final String url;
    public final Integer port;
    public final Integer retryMax;
    public final Integer retryBackoffMs;

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public DatadogLogsSinkConnectorConfig(Map<String, String> props) {
        super(baseConfigDef(), props);
        ddTags = getTags(DD_TAGS);
        ddService = getString(DD_SERVICE);
        ddHostname = getString(DD_HOSTNAME);
        ddApiKey = getPasswordValue(DD_API_KEY);
        url = getString(URL);
        port = getInt(PORT);
        retryMax = getInt(RETRY_MAX);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        validateConfig();
    }

    private void validateConfig() {
        if (getPasswordValue(DD_API_KEY) == null) {
            throw new ConfigException("API Key must not be empty.");
        }
    }

    private static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addDatadogConfigs(configDef);
        addProxyConfigs(configDef);
        addRetryConfigs(configDef);
        return configDef;
    }

    private static void addDatadogConfigs(ConfigDef configDef) {
        int orderInGroup = 0;
        final String group = "Datadog";

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
        final String group = "Proxy";

        configDef.define(
                URL,
                Type.STRING,
                "http-intake.logs.datadoghq.com",
                Importance.LOW,
                "Proxy endpoint when logs are not directly forwarded to Datadog.",
                group,
                ++orderInGroup,
                Width.LONG,
                "URL"
        ).define(
                PORT,
                Type.INT,
                443,
                Range.between(1, 65535),
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
        final String group = "Retry";

        configDef.define(
                RETRY_MAX,
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