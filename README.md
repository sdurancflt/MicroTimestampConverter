# Custom Timestamp Converter SMT

- [Custom Timestamp Converter SMT](#custom-timestamp-converter-smt)
  - [Setup](#setup)
    - [Start Docker Compose](#start-docker-compose)
    - [Install JDBC Sink Connector plugin](#install-jdbc-sink-connector-plugin)
  - [Reproduce Issue](#reproduce-issue)
    - [Register Schema](#register-schema)
    - [Run the Producer](#run-the-producer)
    - [Sink Connector](#sink-connector)
  - [Custom SMT and DB Trigger Workaround](#custom-smt-and-db-trigger-workaround)
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

Check that we are using for the URL host.docker.internal to point to the host from inside the container.

If we check our database and look for the table customer rows we will see the entries keep only microseconds resolution.

The issue is that currently TimestampConverter relies on java.util.Date and SimpleDateFormat both with resolution till milliseconds.

## Custom SMT and DB Trigger Workaround 

Create a new table in postgres:

```sql
create table customers2 (first_name text, last_name text, customer_time text,
						 customer_time_final timestamp without time zone);
```

You will also need to create a trigger:

```sql
CREATE FUNCTION time_stamp() RETURNS trigger AS $time_stamp$
    BEGIN
        IF NEW.customer_time IS NULL THEN
            RAISE EXCEPTION 'customer_time cannot be null';
        END IF;
        NEW.customer_time_final := TO_TIMESTAMP(SUBSTRING(NEW.customer_time,1,26),'yyyy-MM-dd HH:MI:SS.US');
        RETURN NEW;
    END;
$time_stamp$ LANGUAGE plpgsql;

CREATE TRIGGER time_stamp BEFORE INSERT OR UPDATE ON customers2
    FOR EACH ROW EXECUTE FUNCTION time_stamp();
```

Now let's try to use our custom SMT:

```bash
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/my-sink-postgres2/config \
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
            "transforms.timesmod.target.type": "string",
            "transforms.timesmod.type": "io.confluent.csta.timestamp.transforms.TimestampConverterMicro$Value",
             "transforms.timesmod.format": "yyyy-MM-dd HH:mm:ss.nnnnnn",
            "transforms.timesmod.unix.precision": "microseconds"}'
```

This way it should get populated the new table `customers2` with micro seconds resolution.

This custom class is an example of an implementation leveraging java.time.Instant and DateTimeFormatter so 'in principle' allowing for higher resolution. But unfortunately the target type Timestamp cannot be directy used cause Connect internally still expects in such case a java.util.Date and not java.time.Instant... and we would loose the precision beyond milliseconds if we changed this implementation to fit that anyway... So we keep in this example the implementation as it is, as an exploration example, even if a target type as Timestamp won't work with it right now.

 But we leverage the string format (with a value we can use here but not applicable for SimpleDateFormat used in old standard TimestampConverter) and a trigger on database side to workaround the issue.

## Cleanup

From the root of the project:

```bash
docker compose down -v
```
