import json
import logging
from kafka import KafkaConsumer
from datetime import datetime
from clickhouse.clickhouse_db_connection import get_clickhouse_client
from clickhouse.clickhouse_loader_sql_queries import (
    customer_table_load,
    sales_table_load,
    employee_table_load,
    sales_territory_table_load
)
from clickhouse.clickhouse_events_extractor import (
    customer_data_extractor,
    sales_data_extractor,
    employee_data_extractor,
    sales_territory_data_extracor
)

logging.basicConfig(level=logging.INFO)

def consume_and_load():
    client = get_clickhouse_client()
    dbz_consumer = KafkaConsumer(
        'fulfillment.public.customer', 'fulfillment.public.sales', 'fulfillment.public.employee', 'fulfillment.public.sales_territory',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='debezium-streams',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        session_timeout_ms=60000,  
        heartbeat_interval_ms=20000,  
        max_poll_interval_ms=300000  
    )
    employees = []
    try:
        for msg in dbz_consumer:
            topic = msg.topic
            msg_val = msg.value["payload"]["after"]
            logging.info(f"Received message from topic {topic}: {msg_val}")
            try:
                if topic == 'fulfillment.public.employee':
                    employee = msg_val["employee_id"]
                    employees.append(employee)
                    client.command(f"{employee_table_load} ({employee_data_extractor(data=msg_val)})")
                elif topic == 'fulfillment.public.sales_territory':
                    client.command(f"{sales_territory_table_load} ({sales_territory_data_extracor(data=msg_val)})")
                elif topic == 'fulfillment.public.customer':
                    client.command(f"{customer_table_load} ({customer_data_extractor(data=msg_val)})") 
                elif topic == 'fulfillment.public.sales':
                    client.command(f"{sales_table_load} ({sales_data_extractor(data=msg_val)})")

                logging.info(f"Successfully loaded record from topic {topic} to Clickhouse")
            except Exception as e:
                logging.error(f"Error loading record from topic {topic}: {e}")
    except Exception as e:
        logging.error(f"Consumer loop error: {e}")
    finally:
        dbz_consumer.close()
        client.close()
        logging.info("Database connection closed")

def main():
    while True:
        try:
            consume_and_load()
        except Exception as e:
            logging.error(f"Exception in main loop: {e}")
            logging.info("Reconnecting...")

if __name__ == '__main__':
    main()
