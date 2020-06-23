package com.datadoghq;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.github.jcustenborder.kafka.connect.utils.transformation.BaseKeyValueTransformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Map;

@Title("Super Cool Transformation")
@Description("This transformation will change one record to another record.")
public class DatadogKeyValueTransformation<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  DatadogKeyValueTransformationConfig config;

  protected DatadogKeyValueTransformation(boolean isKey) {
    super(isKey);
  }

  @Override
  public ConfigDef config() {
    return DatadogKeyValueTransformationConfig.config();
  }

  @Override
  public void close() {

  }

  @Override
  protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
    throw new UnsupportedOperationException("This method will execute against byte arrays.");
  }

  @Override
  protected SchemaAndValue processString(R record, Schema inputSchema, String input) {
    throw new UnsupportedOperationException("This method will execute against Strings.");
  }

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new DatadogKeyValueTransformationConfig(map);
  }

  /**
   * This implementation works against the key of the record.
   * @param <R>
   */
  public static class Key<R extends ConnectRecord<R>> extends DatadogKeyValueTransformation<R> {
    public Key() {
      super(true);
    }
  }

  /**
   * This implementation works against the value of the record.
   * @param <R>
   */
  public static class Value<R extends ConnectRecord<R>> extends DatadogKeyValueTransformation<R> {
    public Value() {
      super(false);
    }
  }
}