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
      "connector.class": "com.datadoghq.connect.logs",
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

## Deployment

See [Splunk Docs](https://docs.splunk.com/Documentation/KafkaConnect/latest/User/ConfigureSplunkKafkaConnect) to learn more about deployment options.

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
   "splunk.indexes": "<list-of-indexes-for-topics-data-separated-by-comma>",
   "splunk.sources": "<list-of-sources-for-topics-data-separated-by-comma>",
   "splunk.sourcetypes": "<list-of-sourcetypes-for-topics-data-separated-by-comma>",
   "splunk.hec.uri": "<Splunk-HEC-URI>",
   "splunk.hec.token": "<Splunk-HEC-Token>",
   "splunk.hec.raw": "<true|false>",
   "splunk.hec.raw.line.breaker": "<line breaker separator>",
   "splunk.hec.json.event.enrichment": "<key value pairs separated by comma, only applicable to /event HEC>",
   "splunk.hec.ack.enabled": "<true|false>",
   "splunk.hec.ack.poll.interval": "<event ack poll interval>",
   "splunk.hec.ack.poll.threads": "<number of threads used to poll event acks>",
   "splunk.hec.ssl.validate.certs": "<true|false>",
   "splunk.hec.http.keepalive": "<true|false>",
   "splunk.hec.max.http.connection.per.channel": "<max number of http connections per channel>",
   "splunk.hec.total.channels": "<total number of channels>",
   "splunk.hec.max.batch.size": "<max number of kafka records post in one batch>",
   "splunk.hec.threads": "<number of threads to use to do HEC post for single task>",
   "splunk.hec.event.timeout": "<timeout in seconds>",
   "splunk.hec.socket.timeout": "<timeout in seconds>",
   "splunk.hec.track.data": "<true|false, tracking data loss and latency, for debugging lagging and data loss>"
   "splunk.header.support": "<true|false>",
   "splunk.header.custom": "<list-of-custom-headers-to-be-used-from-kafka-headers-separated-by-comma>", 
   "splunk.header.index": "<header-value-to-be-used-as-splunk-index>",
   "splunk.header.source": "<header-value-to-be-used-as-splunk-source>",
   "splunk.header.sourcetype": "<header-value-to-be-used-as-splunk-sourcetype>",
   "splunk.header.host": "<header-value-to-be-used-as-splunk-host>",
   "splunk.hec.json.event.formatted": "<true|false>",
   "splunk.hec.ssl.trust.store.path": "<Java KeyStore location>",
   "splunk.hec.ssl.trust.store.password": "<Java KeyStore password>"
  }
}
```

### Parameters

#### Required Parameters
| Name              | Description                | Default Value  |
|--------           |----------------------------|-----------------------|
|`name` | Connector name. A consumer group with this name will be created with tasks to be distributed evenly across the connector cluster nodes.|
| `connector.class` | The Java class used to perform connector jobs. Keep the default unless you modify the connector.|`com.splunk.kafka.connect.SplunkSinkConnector`|
| `tasks.max` |  The number of tasks generated to handle data collection jobs in parallel. The tasks will be spread evenly across all Splunk Kafka Connector nodes.||
| `splunk.hec.uri` | Splunk HEC URIs. Either a list of FQDNs or IPs of all Splunk indexers, separated with a ",", or a load balancer. The connector will load balance to indexers using round robin. Splunk Connector will round robin to this list of indexers. `https://hec1.splunk.com:8088,https://hec2.splunk.com:8088,https://hec3.splunk.com:8088`||
| `topics` |  Comma separated list of Kafka topics for Splunk to consume. `prod-topic1,prod-topc2,prod-topic3`||
#### General Optional Parameters
| Name              | Description                | Default Value  |
|--------           |----------------------------|-----------------------|
| `splunk.indexes` | Target Splunk indexes to send data to. It can be a list of indexes which shall be the same sequence / order as topics. It is possible to inject data from different kafka topics to different splunk indexes. For example, prod-topic1,prod-topic2,prod-topic3 can be sent to index prod-index1,prod-index2,prod-index3. If you would like to index all data from multiple topics to the main index, then "main" can be specified. Leaving this setting unconfigured will result in data being routed to the default index configured against the HEC token being used. Verify the indexes configured here are in the index list of HEC tokens, otherwise Splunk HEC will drop the data. |`""`|
| `splunk.sources` |  Splunk event source metadata for Kafka topic data. The same configuration rules as indexes can be applied. If left unconfigured, the default source binds to the HEC token. | `""` |
| `splunk.sourcetypes` | Splunk event sourcetype metadata for Kafka topic data. The same configuration rules as indexes can be applied here. If left unconfigured, the default source binds to the HEC token. | `""` |
| `splunk.flush.window` | The interval in seconds at which the events from kafka connect will be flushed to Splunk. | `30` |
| `splunk.hec.ssl.validate.certs` | Valid settings are `true` or `false`. Enables or disables HTTPS certification validation. |`true`|
| `splunk.hec.http.keepalive` | Valid settings are `true` or `false`. Enables or disables HTTP connection keep-alive. |`true`|
| `splunk.hec.max.http.connection.per.channel` | Controls how many HTTP connections will be created and cached in the HTTP pool for one HEC channel. |`2`|
| `splunk.hec.total.channels` | Controls the total channels created to perform HEC event POSTs. See the Load balancer section for more details. |`2`|
| `splunk.hec.max.batch.size` | Maximum batch size when posting events to Splunk. The size is the actual number of Kafka events, and not byte size. |`500`|
| `splunk.hec.threads` | Controls how many threads are spawned to do data injection via HEC in a **single** connector task. |`1`|
| `splunk.hec.socket.timeout` | Internal TCP socket timeout when connecting to Splunk. Value is in seconds. |`60`|
| `splunk.hec.ssl.trust.store.path` | Location of Java KeyStore. |`""`|
| `splunk.hec.ssl.trust.store.password` | Password for Java KeyStore. |`""`|
| `splunk.hec.json.event.formatted` | Set to `true` for events that are already in HEC format. Valid settings are `true` or `false`. |`false`|
| `splunk.hec.max.outstanding.events` | Maximum amount of un-acknowledged events kept in memory by connector. Will trigger back-pressure event to slow down collection if reached. | `1000000` |
| `splunk.hec.max.retries` | Amount of times a failed batch will attempt to resend before dropping events completely. Warning: This will result in data loss, default is `-1` which will retry indefinitely  | `-1` |
| `splunk.hec.backoff.threshhold.seconds` | The amount of time Splunk Connect for Kafka waits to attempt resending after errors from a HEC endpoint." | `60` |


## License

Datadog Kafka Connect Logs is licensed under the Apache License 2.0. Details can be found in the file LICENSE.

