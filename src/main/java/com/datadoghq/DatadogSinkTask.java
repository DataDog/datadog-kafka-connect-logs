package com.datadoghq;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

public class DatadogSinkTask extends SinkTask {
  /*
    Your connector should never use System.out for logging. All of your classes should use slf4j
    for logging
 */
  private static Logger log = LoggerFactory.getLogger(DatadogSinkTask.class);

  DatadogSinkConnectorConfig config;
  @Override
  public void start(Map<String, String> settings) {
    this.config = new DatadogSinkConnectorConfig(settings);
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
