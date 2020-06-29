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

  //TODO: Add remaining config options from logstash example


  public DatadogSinkConnectorConfig(Map<?, ?> originals) {
    super(baseConfigDef(), originals);
  }

  private static ConfigDef baseConfigDef() {
      final ConfigDef configDef = new ConfigDef();
      addMetadataConfigs(configDef);
      return configDef;
  }

  private static void addMetadataConfigs(ConfigDef configDef) {
    int orderInGroup = 0;
    final String group = "Metadata";

    // TODO: Add Metadata Configs
}
