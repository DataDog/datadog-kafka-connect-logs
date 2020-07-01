package com.datadoghq.connect.datadog.logs.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

public class DatadogLogsSinkTask extends SinkTask {
  /*
    Your connector should never use System.out for logging. All of your classes should use slf4j
    for logging
 */
  private static Logger log = LoggerFactory.getLogger(DatadogLogsSinkTask.class);

  DatadogLogsSinkConnectorConfig config;
  @Override
  public void start(Map<String, String> settings) {
    this.config = new DatadogLogsSinkConnectorConfig(settings);
    //TODO: Create resources like database or api connections here.
  }

  @Override
  public void put(Collection<SinkRecord> records) {

  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  @Override
  public void stop() {
    //Close resources here.
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
