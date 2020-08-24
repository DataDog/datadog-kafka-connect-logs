# Datadog Kafka Connect Logs

datadog-kafka-connect-logs is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect) for sending 
records from Kafka to the Datadog Event Intake API.

## Requirements

1. Kafka version 1.0.0 and above.
2. Java 8 and above.
3. Confluent Platform 4.0.x and above (optional).

## Installation and Setup
### Install from Confluent Hub

See [confluent's documentation](https://docs.confluent.io/current/connect/managing/install.html).

### Download from Maven

Download under `Direct Downloads` from [JFrog/Bintray](https://bintray.com/beta/#/datadog/datadog-maven/datadog-kafka-connect-logs?tab=overview).

### Build from Source

1. Clone the repo from https://github.com/DataDog/datadog-kafka-connect-logs
2. Verify that Java8 JRE or JDK is installed.
3. Run `mvn clean compile package`. This will build the jar in the `/target` directory. The name will be `datadog-kafka-connect-logs-[VERSION].jar`.
4. The jar file for use on [Confluent Hub](https://www.confluent.io/hub/) can be found in `target/components/packages`.

## Quick Start

1. [Start](https://kafka.apache.org/quickstart) your Kafka Cluster and confirm it is running.
2. If this is a new install, create a test topic (eg: `perf`). Inject events into the topic. This can be done using the Kafka-bundled [kafka-console-producer](https://kafka.apache.org/quickstart#quickstart_send).
3. Within your Kafka Connect deployment adjust the values for `bootstrap.servers` and `plugin.path` inside the `$KAFKA_HOME/config/connect-distributed.properties` file. `bootstrap.servers` should be configured to point to your Kafka Brokers. `plugin.path` should be configured to point to the install directory of your Kafka Connect Sink and Source Connectors.
4. Place the jar file created by `mvn package` (`datadog-kafka-connect-logs-[VERSION].jar`) in or under the location specified in `plugin.path` 
5. Run `.$KAFKA_HOME/bin/connect-distributed.sh $KAFKA_HOME/config/connect-distributed.properties` to start Kafka Connect.
6. Run the following command to create connector tasks. Adjust `topics` to configure the Kafka topic to be ingested and
set your Datadog `api_key`.

```
  curl localhost:8083/connectors -X POST -H "Content-Type: application/json" -d '{
    "name": "datadog-kafka-connect-logs",
    "config": {
      "connector.class": "com.datadoghq.connect.logs.DatadogLogsSinkConnector",
      "tasks.max": "3",
      "topics":"<YOUR_TOPIC>",
    }
  }'    
```

7. You can verify that data is ingested to the Datadog platform by searching for `kafka-connect` as the `ddsource`.
8. Use the following commands to check status, and manage connectors and tasks:

```
    # List active connectors
    curl http://localhost:8083/connectors

    # Get datadog-kafka-connect-logs connector info
    curl http://localhost:8083/connectors/datadog-kafka-connect-logs

    # Get datadog-kafka-connect-logs connector config info
    curl http://localhost:8083/connectors/datadog-kafka-connect-logs/config

    # Delete datadog-kafka-connect-logs connector
    curl http://localhost:8083/connectors/datadog-kafka-connect-logs -X DELETE

    # Get kdatadog-kafka-connect-logs connector task info
    curl http://localhost:8083/connectors/datadog-kafka-connect-logs/tasks
```

See the [the Confluent documentation](https://docs.confluent.io/current/connect/managing.html#common-rest-examples) for additional REST examples.

## Configuration

After Kafka Connect is brought up on every host, all of the Kafka Connect instances will form a cluster automatically.
A REST call can be executed against one of the cluster instances, and the configuration will automatically propagate to all instances in the cluster.

### Configuration schema structure
Use the below schema to configure Splunk Connect for Kafka

```
{
"name": "<connector-name>",
"config": {
   "connector.class": "com.splunk.kafka.connect.SplunkSinkConnector",
   "tasks.max": "<number-of-tasks>",
   "topics": "<list-of-topics-separated-by-comma>",
   
  }
}
```

### Parameters

#### Required Parameters
| Name              | Description                | Default Value  |
|--------           |----------------------------|-----------------------|
|`name` | Connector name. A consumer group with this name will be created with tasks to be distributed evenly across the connector cluster nodes.|
| `connector.class` | The Java class used to perform connector jobs. Keep the default unless you modify the connector.|`com.datadoghq.connect.logs.DatadogLogsSinkConnector`|
| `tasks.max` |  The number of tasks generated to handle data collection jobs in parallel. The tasks will be spread evenly across all Datadog Kafka Connector nodes.||
| `topics` |  Comma separated list of Kafka topics for Datadog to consume. `prod-topic1,prod-topc2,prod-topic3`||
| `datadog.api_key` | API key to access Datadog API.||
#### General Optional Parameters
| Name              | Description                | Default Value  |
|--------           |----------------------------|-----------------------|



## License

Datadog Kafka Connect Logs is licensed under the Apache License 2.0. Details can be found in the file LICENSE.

