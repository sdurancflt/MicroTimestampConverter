# Custom Timestamp Converter SMT

Currently TimestampConverter looses precision beyond milliseconds during sinking to an external database. We present here an example of implementation that allows this precision.

- [Custom Timestamp Converter SMT](#custom-timestamp-converter-smt)
  - [Setup](#setup)
    - [Start Docker Compose](#start-docker-compose)
    - [Install JDBC Sink Connector plugin](#install-jdbc-sink-connector-plugin)
  - [Reproduce Issue](#reproduce-issue)
    - [Register Schema](#register-schema)
    - [Run the Producer](#run-the-producer)
    - [Sink Connector](#sink-connector)
  - [Custom SMT](#custom-smt)
  - [Cleanup](#cleanup)

## Setup

### Start Docker Compose

```bash
docker compose up -d
```

Check logs for confirming all services are running:

```bash
docker compose logs -f
```

### Install JDBC Sink Connector plugin

```bash
docker compose exec -it connect bash
```

Once inside the container we can install a new connector from confluent-hub:

```bash
confluent-hub install confluentinc/kafka-connect-jdbc:latest
```

(Choose option 2 and after say yes to everything when prompted.)

Now we need to restart our connect:

```bash
docker compose restart connect
```

Now if we list our plugins we should see two new ones corresponding to the JDBC connector.

```bash
curl localhost:8083/connector-plugins | jq
```

## Reproduce Issue

### Register Schema

Lets register our schema against Schema Registry:

```bash
jq '. | {schema: tojson}' src/main/resources/avro/customer.avsc | \
curl -X POST http://localhost:8081/subjects/customers-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d @-
```

### Run the Producer

Now let's run our producer io.confluent.csta.timestamp.avro.AvroProducer.

And check with consumer:

```bash
kafka-avro-console-consumer --topic customers \
--bootstrap-server 127.0.0.1:9092 \
--property schema.registry.url=http://127.0.0.1:8081 \
--from-beginning
```

Just some entries should be enough so you can stop after a while.


### Sink Connector

Now let's create our sink connector:

```bash
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/my-sink-postgres/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "customers",
          "tasks.max"          : "1",
          "auto.create"        : "true",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
            "transforms": "timesmod",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod.unix.precision": "microseconds"}'
```

If we check our database and look for the table customer rows we will see the entries keep only milliseconds resolution.

The issue is that currently TimestampConverter relies on java.util.Date and SimpleDateFormat both with resolution till milliseconds.

## Custom SMT 

Now let's try to use our custom SMT:

```bash
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/my-sink-postgres3/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "customers",
          "table.name.format"  : "${topic}2",
          "tasks.max"          : "1",
          "auto.create"        : "true",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
            "transforms": "timesmod",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.type": "io.confluent.csta.timestamp.transforms.TimestampConverterMicro$Value",
            "transforms.timesmod.unix.precision": "microseconds"}'
```

This way it should get populated the new table `customers2` with micro seconds resolution.

This custom SMT class `io.confluent.csta.timestamp.transforms.TimestampConverterMicro` is an example of an implementation leveraging java.time.Instant and java.sql.Timestamp (an implementation of java.util.Date with precision beyond milliseconds) allowing for higher resolution. 

## Cleanup

From the root of the project:

```bash
docker compose down -v
```
