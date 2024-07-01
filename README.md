# Custom Timestamp Converter SMT

Currently TimestampConverter looses precision beyond milliseconds during sinking to an external database. We present here a first exploration of the problem and possible workarounds.

- [Custom Timestamp Converter SMT](#custom-timestamp-converter-smt)
  - [Setup](#setup)
    - [Start Docker Compose](#start-docker-compose)
    - [Install JDBC Sink Connector plugin](#install-jdbc-sink-connector-plugin)
  - [Reproduce Issue](#reproduce-issue)
    - [Register Schema](#register-schema)
    - [Run the Producer](#run-the-producer)
    - [Sink Connector](#sink-connector)
  - [Custom SMT and DB Trigger](#custom-smt-and-db-trigger)
  - [No Custom SMT just DB Trigger](#no-custom-smt-just-db-trigger)
  - [Source JDBC Connector](#source-jdbc-connector)
    - [Default Behaviour](#default-behaviour)
    - [TimestampConverter SMT](#timestampconverter-smt)
    - [Max Value Issue](#max-value-issue)
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

## Custom SMT and DB Trigger

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
        NEW.customer_time_final := TO_TIMESTAMP(SUBSTRING(NEW.customer_time,1,26),'yyyy-MM-dd HH24:MI:SS.US');
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

This custom SMT class `io.confluent.csta.timestamp.transforms.TimestampConverterMicro` is an example of an implementation leveraging java.time.Instant and DateTimeFormatter so 'in principle' allowing for higher resolution. But unfortunately the target type Timestamp cannot be directy used cause Connect internally still expects in such case a java.util.Date and not java.time.Instant... So we keep in this example the implementation as it is, as an exploration example, even if a target type as Timestamp won't work with it right now.

But we leverage the string target type (with a format value we can use here but not applicable for SimpleDateFormat used in old standard TimestampConverter) and a trigger on database side to workaround the issue.

## No Custom SMT just DB Trigger

Create a new table in posgres:

```sql
create table customers3 (first_name text, last_name text, customer_time bigint,
						 customer_time_final timestamp without time zone);
```

And the corresponding trigger:

```sql
CREATE FUNCTION time_stamp3() RETURNS trigger AS $time_stamp3$
    BEGIN
        -- Check that empname and salary are given
        IF NEW.customer_time IS NULL THEN
            RAISE EXCEPTION 'customer_time cannot be null';
        END IF;

        -- Remember who changed the payroll when
        NEW.customer_time_final := to_timestamp(NEW.customer_time::double precision/1000/1000);
        RETURN NEW;
    END;
$time_stamp3$ LANGUAGE plpgsql;

CREATE TRIGGER time_stamp3 BEFORE INSERT OR UPDATE ON customers3
    FOR EACH ROW EXECUTE FUNCTION time_stamp3();
```

Now let's create the connector with no SMT:

```shell
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/my-sink-postgres3/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "customers",
          "table.name.format"  : "${topic}3",
          "tasks.max"          : "1",
          "auto.create"        : "true",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter"}'
```

This way it should get populated the new table `customers3` with micro seconds resolution.

## Source JDBC Connector

### Default Behaviour

Let's create the table:

```sql
create table customers100 (first_name text not null, last_name text not null,customer_time timestamp without time zone not null);
```

Let's insert a value on it:

```sql
INSERT INTO customers100(
	first_name, last_name, customer_time)
	VALUES ('rui', 'fernandes',   now());
```

Define a JDBC source connector for table customers3:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/my-source-postgres/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "customers100",
          "timestamp.granularity": "nanos_long",
             "mode":"bulk"}'
```

You can see the topic `postgres-customers100` getting created with schema:

```json
{
  "connect.name": "customers100",
  "fields": [
    {
      "name": "first_name",
      "type": "string"
    },
    {
      "name": "last_name",
      "type": "string"
    },
    {
      "name": "customer_time",
      "type": "long"
    }
  ],
  "name": "customers100",
  "type": "record"
}
```

Also checking the messages of the topic:

```json
{
  "first_name": "rui",
  "last_name": "fernandes",
  "customer_time": "1719780510259008000"
}
```

Which is the original time with microseconds precision now with nanoseconds (the last 3 zeros) but it does not have the right timestamp logical type.

### TimestampConverter SMT

Next we could try to use default timestamp converter for the field and see what happens in each case:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/my-source2-postgres/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres2-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "customers100",
             "mode":"bulk",
             "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
          "timestamp.granularity": "nanos_long",
          "transforms": "timesmod",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod.unix.precision": "microseconds"}'
```

In this case we end up with a following schema:

```json
{
  "connect.name": "customers100",
  "fields": [
    {
      "name": "first_name",
      "type": "string"
    },
    {
      "name": "last_name",
      "type": "string"
    },
    {
      "name": "customer_time",
      "type": {
        "connect.name": "org.apache.kafka.connect.data.Timestamp",
        "connect.version": 1,
        "logicalType": "timestamp-millis",
        "type": "long"
      }
    }
  ],
  "name": "customers100",
  "type": "record"
}
```

And message like this:

```json
{
  "first_name": "rui",
  "last_name": "fernandes",
  "customer_time": 1719780510259008
}
```

We have now microseconds resolution but schema is created with millis. We can evolve the schema to use micros resolution:

```json
{
  "connect.name": "customers100",
  "fields": [
    {
      "name": "first_name",
      "type": "string"
    },
    {
      "name": "last_name",
      "type": "string"
    },
    {
      "name": "customer_time",
      "type": {
        "connect.name": "org.apache.kafka.connect.data.Timestamp",
        "connect.version": 1,
        "logicalType": "timestamp-micros",
        "type": "long"
      }
    }
  ],
  "name": "customers100",
  "type": "record"
}
```

And if we restart the connector we get the message still with micros as:

```json
{
  "first_name": "rui",
  "last_name": "fernandes",
  "customer_time": 1719780510259008
}
```

### Max Value Issue

Let's insert a much bigger date on it:

```sql
INSERT INTO customers100(
	first_name, last_name, customer_time)
	VALUES ('rui', 'fernandes',    timestamp '9999-12-31 23:59:59.000000');
```

Now if we restart our connector. We get a message:

```json
{
  "first_name": "rui",
  "last_name": "fernandes",
  "customer_time": "9223372036854775"
}
```

As one can see we get now a string invalid as per our schema in comparison to what we had before which was a valid long representing the micros. The reason for that is that if we transform our date in database to long we get in milliseconds:

```sql
SELECT *,EXTRACT(EPOCH FROM customer_time)*1000 from customers100;
```

```csv
"first_name","last_name","customer_time","?column?"
"rui","fernandes","2024-06-30 20:48:30.259008","1719780510259.01"
"rui","fernandes","9999-12-31 23:59:59","253402300799000"
```

Which means that for the large date we have in nanoseconds `253402300799000000000` which is above `9223372036854775807` the maximum long allowed. The connector basically brings down the possible max value in nanos to micros removing its final 3 decimals and registers the value as string `"9223372036854775"`.

Curious enough for our tests we find the limit to be not exactly around the `9223372036854775807` (which would be around `Friday, 11 April 2262 23:47:16.854` if you check https://www.epochconverter.com/ for `9223372036854775807`) but a bit before for the behaviour. If we create two entries:

```sql
INSERT INTO customers100(
	first_name, last_name, customer_time)
	VALUES ('rui', 'fernandes',    timestamp '2255-04-11 23:47:16.853000');

INSERT INTO customers100(
	first_name, last_name, customer_time)
	VALUES ('rui', 'fernandes',    timestamp '2256-04-11 23:47:16.855000');
```

And restart our connector.

We find the first one to be transformed correctly into `9002447236853000` while the second (and any above) into a string, in this case `"9034069636855000"`. Although it still process the microseconds correctly (at least until it reaches the maximum value for long in nanoseconds as mentioned before).

It's not clear to us what triggers this behaviour. Starting processing as string even before reaching the limit. In any case what we can try is to transform the value into a long after for the cases it is a string. We can do that by changing our connector with a new smt:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/my-source2-postgres/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres2-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "customers100",
             "mode":"bulk",
             "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
          "timestamp.granularity": "nanos_long",
          "transforms": "timesmod,tolong,timesmod2",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod.unix.precision": "microseconds",
            "transforms.tolong.field": "tolong",
            "transforms.tolong.type": "org.apache.kafka.connect.transforms.Cast$Value",
            "transforms.tolong.spec": "customer_time:int64",
            "transforms.timesmod2.field": "customer_time",
            "transforms.timesmod2.target.type": "Timestamp",
            "transforms.timesmod2.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod2.unix.precision": "microseconds"
            }'
```

With this we get for both `9002447236853` and `9034069636855` but we loose this way the micros precision. And still we can't handle the case of max value:

```sql 
INSERT INTO customers100(
	first_name, last_name, customer_time)
	VALUES ('rui', 'fernandes',    timestamp '9999-12-31 23:59:59.000000');
```

Which will be reduced to `Friday, 11 April 2262 23:47:16.854`.

## Cleanup

From the root of the project:

```bash
docker compose down -v
```
