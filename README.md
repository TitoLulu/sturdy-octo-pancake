# Rembo Data Streaming Project

Welcome to the Rembo Data Streaming Project! This project aims to collect real-time sales data, employee information associated with sales, customer details, and sales region information using various technologies such as Kafka, PostgreSQL, Debezium, Control Center, Schema Registry, ClickHouse, Grafana, PGAdmin, and Zookeeper.

## Setup Instructions

To set up and run the project, follow these steps:

1. **Install Docker**:
   Ensure Docker is installed on your machine.

2. **Trigger Docker Services**:
   Run the following command to start Docker services in detached mode:
   ```bash
   docker-compose up -d
   ```
3. **Create Dabase**
   Use PgAdmin UI to create a postgres database called rembo
   Similaröy use Grafana UI to create a clickhouse sink database called rembo
4. **Create Kafka Connector**
   Run the following bash command to create a Kafka connector
   ```bash
   curl -X POST --location 'http://localhost:8083/connectors?expand=status&expand=info' \
   --header 'Content-Type: application/json' \
   --header 'Accept: application/json' \
   --data '@./connectors/debezium.json'
   ```
5. **Monitoring**
   Use PgAdmin to monitor records created in postgres
   Use Grafana to monitor cdc records from postgres
   Utilize control center to monitor kafka producers, consumers, topics and connectors

6. **Directory Structure**
   ```bash
   rembo_data_streaming_project/
   │
   ├── services/
   │ ├── kafka/
   │ ├── postgres/
   │ ├── debezium/
   │ ├── control_center/
   │ ├── schema_registry/
   │ ├── clickhouse/
   │ ├── grafana/
   │ ├── pgadmin/
   │ └── zookeeper/
   │
   ├── connectors/
   │ └── debezium.json
   │
   ├── config/
   │
   ├── consumers/
   │ ├── cdc_consumers.py
   │ └── events_consumer.py
   │
   ├── datagen/
   │ ├── customer_sales_datagen.py
   │ └── employee_territory_datagen.py
   │
   ├── postgres/
   │ ├── postgres_db_connection.py
   │ └── postgres_sql_queries.py
   │
   └── clickhouse/
   ├── clickhouse_db_connection.py
   └── clickhouse_queries.py
   ```
