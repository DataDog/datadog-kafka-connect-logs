# Kafka Connect Datadog Logs

kafka-connect-datadog-logs is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect) for sending 
records from Kafka to the Datadog Event Intake API.

# Development

You can build kafka-connect-datadog-logs with Maven using the standard lifecycle phases.  To build the connector run 
`mvn clean compile package`, which will create a jar file in the `target` directory that you can use with Kafka Connect.

The jar file for use on [Confluent Hub](https://www.confluent.io/hub/) can be found in `target/components/packages`.

# Contribute

- Source Code: https://github.com/DataDog/kafka-connect-datadog-logs
- Issue Tracker: https://github.com/DataDog/kafka-connect-datadog-logs/issues

# License

The project is licensed under the Apache 2 license.
