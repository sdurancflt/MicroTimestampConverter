# Custom Timestamp Converter SMT

- [Custom Timestamp Converter SMT](#custom-timestamp-converter-smt)
  - [Setup](#setup)
    - [Start Docker Compose](#start-docker-compose)
    - [Check Control Center](#check-control-center)
    - [Install JDBC Sink Connector plugin](#install-jdbc-sink-connector-plugin)
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

### Check Control Center

Open http://localhost:9021 and check cluster is healthy

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

## Cleanup

From the root of the project:

```bash
docker compose down -v
```
