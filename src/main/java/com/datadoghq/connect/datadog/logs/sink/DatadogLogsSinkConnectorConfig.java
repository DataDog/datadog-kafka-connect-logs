package com.datadoghq.connect.datadog.logs.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonEmptyStringWithoutControlChars;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

public class DatadogLogsSinkConnectorConfig extends AbstractConfig {

    public static final String DATADOG_PORT = "datadog.port";
    private static final String DATADOG_PORT_DOC = "A proxy port for when logs are not directly forwarded to Datadog.";
    private static final int DATADOG_PORT_DEFAULT = 443;
    private static final String DATADOG_PORT_DISPLAY = "Port";

    public static final String DATADOG_API_KEY = "datadog.api_key";
    private static final String DATADOG_API_KEY_DOC = "The API key of your Datadog platform.";
    private static final String DATADOG_API_KEY_DISPLAY = "API Key";

    public static final String DATADOG_URL = "datadog.url";
    private static final String DATADOG_URL_DOC = "The url to send logs data to.";
    private static final String DATADOG_URL_DEFAULT = "http-intake.logs.datadoghq.com";
    private static final String DATADOG_URL_DISPLAY = "DATADOG_URL";

    public static final String COMPRESSION_ENABLE = "compression.enable";
    private static final String COMPRESSION_ENABLE_DOC = "Enable log compression for HTTP.";
    private static final Boolean COMPRESSION_ENABLE_DEFAULT = true;
    private static final String COMPRESSION_ENABLE_DISPLAY = "Enable Compression";

    public static final String COMPRESSION_LEVEL = "compression.level";
    private static final String COMPRESSION_LEVEL_DOC =
            "Set the log compression level for HTTP (1 to 9, 9 being the best ratio).";
    private static final int COMPRESSION_LEVEL_DEFAULT = 6;
    private static final String COMPRESSION_LEVEL_DISPLAY = "Compression Level";

    public static final String RETRY_MAX = "retry.max";
    private static final String RETRY_MAX_DOC = "The number of retries before the output plugin stops.";
    private static final int RETRY_MAX_DEFAULT = 5;
    private static final String RETRY_MAX_DISPLAY = "Max Retries";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
    private static final String RETRY_BACKOFF_MS_DOC =
            "The time in milliseconds to wait following an error before a retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    // Respect limit documented at https://docs.datadoghq.com/api/?lang=bash#logs
    public final Integer ddMaxBatchLength = 500;

    // Connection configs
    public final Integer ddPort;
    public final String ddAPIKey;
    public final String ddURL;
    public final Boolean compressionEnable;
    public final Integer compressionLevel;
    public final Integer retryMax;
    public final Integer retryBackoffMS;

    private static final String DATADOG_GROUP = "Datadog";
    private static final String COMPRESSION_GROUP = "Compression";
    private static final String RETRY_GROUP = "Retry";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Datadog
            .define(
                    DATADOG_PORT,
                    Type.INT,
                    DATADOG_PORT_DEFAULT,
                    Range.between(1, 65535),
                    Importance.LOW,
                    DATADOG_PORT_DOC,
                    DATADOG_GROUP,
                    1,
                    Width.LONG,
                    DATADOG_PORT_DISPLAY
            )
            .define(
                    DATADOG_API_KEY,
                    Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    NonEmptyStringWithoutControlChars.nonEmptyStringWithoutControlChars(),
                    Importance.HIGH,
                    DATADOG_API_KEY_DOC,
                    DATADOG_GROUP,
                    2,
                    Width.LONG,
                    DATADOG_API_KEY_DISPLAY
            )
            .define(
                    DATADOG_URL,
                    Type.STRING,
                    DATADOG_URL_DEFAULT,
                    Importance.LOW,
                    DATADOG_URL_DOC,
                    DATADOG_GROUP,
                    3,
                    Width.LONG,
                    DATADOG_URL_DISPLAY
            )

            // Compression
            .define(
                    COMPRESSION_ENABLE,
                    Type.BOOLEAN,
                    COMPRESSION_ENABLE_DEFAULT,
                    Importance.LOW,
                    COMPRESSION_ENABLE_DOC,
                    COMPRESSION_GROUP,
                    1,
                    Width.LONG,
                    COMPRESSION_ENABLE_DISPLAY
            )
            .define(
                    COMPRESSION_LEVEL,
                    Type.INT,
                    COMPRESSION_LEVEL_DEFAULT,
                    Range.between(1, 9),
                    Importance.LOW,
                    COMPRESSION_LEVEL_DOC,
                    COMPRESSION_GROUP,
                    2,
                    Width.LONG,
                    COMPRESSION_LEVEL_DISPLAY
            )

            // Retries
            .define(
                    RETRY_MAX,
                    Type.INT,
                    RETRY_MAX_DEFAULT,
                    Importance.LOW,
                    RETRY_MAX_DOC,
                    RETRY_GROUP,
                    1,
                    Width.LONG,
                    RETRY_MAX_DISPLAY
            )
            .define(
                    RETRY_BACKOFF_MS,
                    Type.INT,
                    RETRY_BACKOFF_MS_DEFAULT,
                    Importance.LOW,
                    RETRY_BACKOFF_MS_DOC,
                    RETRY_GROUP,
                    2,
                    Width.LONG,
                    RETRY_BACKOFF_MS_DISPLAY
            );

    /**
     * Constructs a configuration option from config passed as file or through the Connect API.
     * @param props are configuration properties passed through a config file or through the Connect API.
     */
    public DatadogLogsSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        ddPort = getInt(DATADOG_PORT);
        ddAPIKey = getString(DATADOG_API_KEY);
        ddURL = getString(DATADOG_URL);
        compressionEnable = getBoolean(COMPRESSION_ENABLE);
        compressionLevel = getInt(COMPRESSION_LEVEL);
        retryMax = getInt(RETRY_MAX);
        retryBackoffMS = getInt(RETRY_BACKOFF_MS);
        validateConfig();
    }

    private void validateConfig() {
        if (ddAPIKey.isEmpty()) {
            throw new ConfigException("API Key must not be empty.");
        }

        if (compressionLevel < 1 || compressionLevel > 9) {
            throw new ConfigException("Please use a compression level between 1 and 9.");
        }
    }
}
