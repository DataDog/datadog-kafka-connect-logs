package com.datadoghq;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class DatadogSinkConnectorConfig extends AbstractConfig {

    public static final String DD_SOURCE = "datadog.source";
    private static final String DD_SOURCE_DOC = "The integration name associated with your log: the technology from which the log originated.";
    private static final String DD_SOURCE_DEFAULT = "";
    private static final String DD_SOURCE_DISPLAY = "Source Metadata";

    public static final String DD_TAGS = "datadog.tags";
    private static final String DD_TAGS_DOC = "Tags associated with your logs in a space separated tag:value format.";
    private static final String DD_TAGS_DEFAULT = "";
    private static final String DD_TAGS_DISPLAY = "Tags Metadata";

    public static final String DD_SERVICE = "datadog.service";
    private static final String DD_SERVICE_DOC = "The name of the application or service generating the log events.";
    private static final String DD_SERVICE_DEFAULT = "";
    private static final String DD_SERVICE_DISPLAY = "Service Metadata";

    public static final String DD_STATUS = "datadog.status";
    private static final String DD_STATUS_DOC = "This corresponds to the level/severity of a log. It is used to define patterns and has a dedicated layout in the Datadog Log UI.";
    private static final String DD_STATUS_DEFAULT = "";
    private static final String DD_STATUS_DISPLAY = "Status Metadata";

    public static final String DD_HOSTNAME = "datadog.hostname";
    private static final String DD_HOSTNAME_DOC = "The name of the originating host of the log.";
    private static final String DD_HOSTNAME_DEFAULT = "";
    private static final String DD_HOSTNAME_DISPLAY = "Hostname Metadata";

    public static final String PORT = "connection.port";
    private static final String PORT_DOC = "A proxy port for when logs are not directly forwarded to Datadog.";
    private static final String PORT_DEFAULT = "443";
    private static final String PORT_DISPLAY = "Port";

    public static final String API_KEY = "connection.api_key";
    private static final String API_KEY_DOC = "The API key of your Datadog platform.";
    private static final String API_KEY_DISPLAY = "API Key";

    public static final String HOSTNAME = "connection.hostname";
    private static final String HOSTNAME_DOC = "The name of the host to send logs data to.";
    private static final String HOSTNAME_DEFAULT = "http-intake.logs.datadoghq.com";
    private static final String HOSTNAME_DISPLAY = "Hostname";

    public static final String USE_SSL = "connection.use_ssl";
    private static final String USE_SSL_DOC = "If true, the agent initializes a secure connection to Datadog. Ensure to update the port if you disable it.";
    private static final String USE_SSL_DEFAULT = "true";
    private static final String USE_SSL_DISPLAY = "Use SSL";

    public static final String USE_HTTP = "connection.use_http";
    private static final String USE_HTTP_DOC = "Enable HTTP forwarding. If you disable it, make sure to update the port to 10516 if use_ssl is enabled or 10514 otherwise.";
    private static final String USE_HTTP_DEFAULT = "true";
    private static final String USE_HTTP_DISPLAY = "Use HTTP";

    public static final String USE_COMPRESSION = "connection.use_compression";
    private static final String USE_COMPRESSION_DOC = "Enable log compression for HTTP.";
    private static final String USE_COMPRESSION_DEFAULT = "true";
    private static final String USE_COMPRESSION_DISPLAY = "Use Compression";

    public static final String COMPRESSION_LEVEL = "connection.compression_level";
    private static final String COMPRESSION_LEVEL_DOC = "Set the log compression level for HTTP (1 to 9, 9 being the best ratio).";
    private static final String COMPRESSION_LEVEL_DEFAULT = "6";
    private static final String COMPRESSION_LEVEL_DISPLAY = "Compression Level";

    public static final String NO_SSL_VALIDATION = "connection.no_ssl_validation";
    private static final String NO_SSL_VALIDATION_DOC = "Disable SSL validation (useful for proxy forwarding).";
    private static final String NO_SSL_VALIDATION_DEFAULT = "false";
    private static final String NO_SSL_VALIDATION_DISPLAY = "No SSL Validation";

    public static final String MAX_RETRIES = "connection.max_retries";
    private static final String MAX_RETRIES_DOC = "The number of retries before the output plugin stops.";
    private static final String MAX_RETRIES_DEFAULT = "5";
    private static final String MAX_RETRIES_DISPLAY = "Max Retries";

    public static final String MAX_BACKOFF = "connection.max_backoff";
    private static final String MAX_BACKOFF_DOC = "The maximum time waited between each retry in seconds.";
    private static final String MAX_BACKOFF_DEFAULT = "30";
    private static final String MAX_BACKOFF_DISPLAY = "Max Backoff";

    public DatadogSinkConnectorConfig(Map<?, ?> originals) {
        super(baseConfigDef(), originals);
        // TODO: Validate configs
    }

    private static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addMetadataConfigs(configDef);
        addConnectionConfigs(configDef);
        return configDef;
    }

    private static void addMetadataConfigs(ConfigDef configDef) {
        int orderInGroup = 0;
        final String group = "Metadata";

        configDef.define(
                DD_SOURCE,
                Type.STRING,
                DD_SOURCE_DEFAULT,
                Importance.MEDIUM,
                DD_SOURCE_DOC,
                group,
                orderInGroup++, ConfigDef.Width.LONG,
                DD_SOURCE_DISPLAY
        ).define(
                DD_TAGS,
                Type.STRING,
                DD_TAGS_DEFAULT,
                Importance.MEDIUM,
                DD_TAGS_DOC,
                group,
                orderInGroup++,
                ConfigDef.Width.LONG,
                DD_TAGS_DISPLAY
        ).define(
                DD_SERVICE,
                Type.STRING,
                DD_SERVICE_DEFAULT,
                Importance.MEDIUM,
                DD_SERVICE_DOC,
                group,
                orderInGroup++,
                ConfigDef.Width.LONG,
                DD_SERVICE_DISPLAY
        ).define(
                DD_STATUS,
                Type.STRING,
                DD_STATUS_DEFAULT,
                Importance.MEDIUM,
                DD_STATUS_DOC,
                group,
                orderInGroup++,
                ConfigDef.Width.LONG,
                DD_STATUS_DISPLAY
        ).define(
                DD_HOSTNAME,
                Type.STRING,
                DD_HOSTNAME_DEFAULT,
                Importance.MEDIUM,
                DD_HOSTNAME_DOC,
                group,
                orderInGroup,
                ConfigDef.Width.LONG,
                DD_HOSTNAME_DISPLAY
        );
    }

    private static void addConnectionConfigs(ConfigDef configDef) {
        int orderInGroup = 0;
        final String group = "Connection";

        // TODO: Add connection configs
    }
}
