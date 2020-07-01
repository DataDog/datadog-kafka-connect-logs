package com.datadoghq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationImportant;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;

/**
 *
 */

@Description("This connector loads Kafka Records and sends them as Datadog Logs to the Datadog Logs Intake API.")
@Title("Datadog Logs Sink Connector")
public class DatadogSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(DatadogSinkConnector.class);
  private Map<String, String> configProperties;

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
      log.info("Setting task configurations for {} workers.", maxTasks);
      List<Map<String, String>> taskConfigs = new ArrayList<>();
      Map<String, String> taskProps = new HashMap<>(configProperties);
      for (int i = 0; i < maxTasks; i++) {
        taskConfigs.add(taskProps);
      }
      return taskConfigs;
  }

  @Override
  public void start(Map<String, String> settings) {
    config = new DatadogSinkConnectorConfig(settings);

    //TODO: Add things you need to do to setup your connector.

    /**
     * This will be executed once per connector. This can be used to handle connector level setup. For
     * example if you are persisting state, you can use this to method to create your state table. You
     * could also use this to verify permissions
     */

  }





  @Override
  public void stop() {
    //TODO: Do things that are necessary to stop your connector.
  }

  @Override
  public ConfigDef config() {
    return DatadogSinkConnectorConfig.CONFIG_DEF;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return DatadogSinkTask.class;
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
