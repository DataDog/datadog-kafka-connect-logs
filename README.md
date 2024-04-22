# Datadog Kafka Connect Logs

`datadog-kafka-connect-logs` is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect) for sending 
records from Kafka as logs to the [Datadog Logs Intake API](https://docs.datadoghq.com/api/v1/logs/).

It is a plugin meant to be installed on a [Kafka Connect Cluster](https://docs.confluent.io/current/connect/) running
besides a [Kafka Broker](https://www.confluent.io/what-is-apache-kafka/).

## Requirements

1. Kafka version 1.0.0 and above.
2. Java 8 and above.
3. Confluent Platform 4.0.x and above (optional).

To install the plugin, one must have a working instance of Kafka Connect connected to a Kafka Broker. See also 
[Confluent's](https://www.confluent.io/product/confluent-platform/) documentation for easily setting this up.

## Installation and Setup
### Install from Confluent Hub

See [Confluent's documentation](https://docs.confluent.io/current/connect/managing/install.html) and the connector's page on
[Confluent Hub](https://www.confluent.io/hub/datadog/kafka-connect-logs).

### Download from Github

Download the latest version from the GitHub [releases page](https://github.com/DataDog/datadog-kafka-connect-logs/releases).
Also see [Confluent's documentation](https://docs.confluent.io/current/connect/managing/community.html) on installing 
community connectors.

### Build from Source

1. Clone the repo from https://github.com/DataDog/datadog-kafka-connect-logs
2. Verify that Java8 JRE or JDK is installed.
3. Run `mvn clean compile package`. This will build the jar in the `/target` directory. The name will be 
`datadog-kafka-connect-logs-[VERSION].jar`.
4. The zip file for use on [Confluent Hub](https://www.confluent.io/hub/) can be found in `target/components/packages`.

## Quick Start

1. To install the plugin, place the plugin's jar file (see [previous section](#installation-and-setup) on how to download or build it)
in or under the location specified in `plugin.path` . If you use Confluent Platform, simply run 
`confluent-hub install target/components/packages/<connector-zip-file>`.
2. Restart your Kafka Connect instance.
3. Run the following command to manually create connector tasks. Adjust `topics` to configure the Kafka topic to be 
ingested and set your Datadog `api_key`.

```
  curl localhost:8083/connectors -X POST -H "Content-Type: application/json" -d '{
    "name": "datadog-kafka-connect-logs",
    "config": {
      "connector.class": "com.datadoghq.connect.logs.DatadogLogsSinkConnector",
      "datadog.api_key": "<YOUR_API_KEY>",
      "tasks.max": "3",
      "topics":"<YOUR_TOPIC>",
    }
  }'    
```

4. You can verify that data is ingested to the Datadog platform by searching for `source:kafka-connect` in the Log 
Explorer tab
5. Use the following commands to check status, and manage connectors and tasks:

```
    # List active connectors
    curl http://localhost:8083/connectors

    # Get datadog-kafka-connect-logs connector info
    curl http://localhost:8083/connectors/datadog-kafka-connect-logs

    # Get datadog-kafka-connect-logs connector config info
    curl http://localhost:8083/connectors/datadog-kafka-connect-logs/config

    # Delete datadog-kafka-connect-logs connector
    curl http://localhost:8083/connectors/datadog-kafka-connect-logs -X DELETE

    # Get datadog-kafka-connect-logs connector task info
    curl http://localhost:8083/connectors/datadog-kafka-connect-logs/tasks
```

See the [the Confluent documentation](https://docs.confluent.io/current/connect/managing.html#common-rest-examples) for additional REST examples.

## Configuration

After Kafka Connect is brought up on every host, all of the Kafka Connect instances will form a cluster automatically.
A REST call can be executed against one of the cluster instances, and the configuration will automatically propagate to all instances in the cluster.

### Parameters

#### Required Parameters
| Name              | Description                | Default Value  |
|--------           |----------------------------|-----------------------|
|`name` | Connector name. A consumer group with this name will be created with tasks to be distributed evenly across the connector cluster nodes.||
| `connector.class` | The Java class used to perform connector jobs. Keep the default unless you modify the connector.|`com.datadoghq.connect.logs.DatadogLogsSinkConnector`||
| `tasks.max` |  The number of tasks generated to handle data collection jobs in parallel. The tasks will be spread evenly across all Datadog Kafka Connector nodes.||
| `topics` |  Comma separated list of Kafka topics for Datadog to consume. `prod-topic1,prod-topic2,prod-topic3`||
| `datadog.api_key` | The API key of your Datadog platform.||
#### General Optional Parameters
| Name              | Description                | Default Value  |
|--------           |----------------------------|-----------------------|
| `datadog.site` | The site of the Datadog intake to send logs to (for example 'datadoghq.eu' to send data to the EU site) | `datadoghq.com` |
| `datadog.url` | Custom Datadog URL endpoint where your logs will be sent. `datadog.url` takes precedence over `datadog.site`. Example: `http-intake.logs.datadoghq.com:443` ||
| `datadog.tags` | Tags associated with your logs in a comma separated tag:value format.||
| `datadog.service` | The name of the application or service generating the log events.||
| `datadog.hostname` | The name of the originating host of the log.||
| `datadog.proxy.url` | Proxy endpoint when logs are not directly forwarded to Datadog.||
| `datadog.proxy.port` | Proxy port when logs are not directly forwarded to Datadog.||
| `datadog.retry.max` | The number of retries before the output plugin stops.| `5` ||
| `datadog.retry.backoff_ms` | The time in milliseconds to wait following an error before a retry attempt is made.| `3000` ||

### Troubleshooting performance

To improve performance of the connector, you can try the following options:

* Update the number of records fetched per poll by setting
  `consumer.override.max.poll.records` in the plugin configuration. This plugin
  sends batches of records synchronously with each poll so a low number of records
  per poll will reduce throughput. Consider setting this to 500 or 1000.
* Increase the number of parallel tasks by adjusting the `tasks.max` parameter.
  Only do this if the hardware is underutilized, such as low CPU, low memory
  usage, and low data injection throughput. Do not set more tasks than
  partitions.
* Increase hardware resources on cluster nodes in case of resource exhaustion,
  such as high CPU, or high memory usage.
* Increase the number of Kafka Connect nodes.

## Single Message Transforms

Kafka Connect supports Single Message Transforms that let you change the structure or content of a message. To 
experiment with this feature, try adding these lines to your sink connector configuration:

```properties
transforms=addExtraField
transforms.addExtraField.type=org.apache.kafka.connect.transforms.InsertField$Value
transforms.addExtraField.static.field=extraField
transforms.addExtraField.static.value=extraValue
```
Now if you restart the sink connector and send some more test messages, each new record should have a `extraField` field 
with value `value`. For more in-depth video, see [confluent's documentation](https://docs.confluent.io/current/connect/transforms/index.html).

## Testing

### Unit Tests

To run the supplied unit tests, run `mvn test` from the root of the project.

### System Tests

We use Confluent Platform for a batteries-included Kafka environment for local testing. Follow the guide 
[here](https://docs.confluent.io/current/quickstart/ce-quickstart.html) to install the Confluent Platform.

Then, install the [Confluent Kafka Datagen Connector](https://github.com/confluentinc/kafka-connect-datagen) to create 
sample data of arbitrary types. Install this Datadog Logs Connector by running 
`confluent-hub install target/components/packages/<connector-zip-file>`.

In the `/test` directory there are some `.json` configuration files to make it easy to create Connectors. There are 
configurations for both the Datagen Connector with various datatypes, as well as the Datadog Logs Connector. To the latter,
you will need to add a valid Datadog API Key for once you upload the `.json` to Confluent Platform.

Once your connectors are set up, you will be able to test them and see relevant data. For performance tests, you can also
use the following command packaged with Confluent platform:

```bash
kafka-producer-perf-test --topic perf-test --num-records 2000000 --record-size 100 --throughput 25000 --producer-props bootstrap.servers=localhost:9092 --print-metrics true
```

## License

Datadog Kafka Connect Logs is licensed under the Apache License 2.0. Details can be found in the file LICENSE.
