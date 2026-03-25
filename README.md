
# Real-Time Network Availability Engine

A high-performance observability pipeline using Go, Kafka, MySQL (CDC), Redis, and ClickHouse.

## Architecture
1. **Simulator (Go)**: Generates random UP/DOWN events into MySQL.
2. **Debezium**: Captures MySQL changes and streams to Kafka.
3. **Processor (Go)**: Consumes Kafka events, tracks state in Redis, and calculates uptime intervals.
4. **ClickHouse**: Stores finalized intervals for high-speed analytical queries.

## Prerequisites
- Docker & Docker Compose
- Go 1.21+

## Quick Start

1. **Start Infrastructure**:
   ```bash
   docker-compose up -d


sudo docker compose  up -d

sudo docker logs -f debezium


sudo docker logs debezium | grep "Exported"







curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
localhost:8083/connectors/ -d '{
  "name": "mysql-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "debezium",
    "database.server.id": "184054",
    "topic.prefix": "dbserver1",
    "database.include.list": "inventory",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.inventory",
    
   
    "schema.history.internal.kafka.recovery.poll.interval.ms": "5000",
    "schema.history.internal.replication.factor": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1"
  }
}'





