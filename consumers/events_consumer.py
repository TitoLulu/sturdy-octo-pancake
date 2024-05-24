import json
import psycopg2
import logging
from kafka import KafkaConsumer
from postgres.postgres_db_connection import get_db_connection
from postgres.postgres_loader_sql_queries import (
    employee_table_load,
    sales_region_load,
    customer_table_load,
    sales_table_load
)
from postgres.postgres_events_extractor import (
    employee_data_extractor,
    sales_territory_data_extractor,
    customer_data_extractor,
    sales_data_extractor
)

logging.basicConfig(level=logging.INFO)

def main():
    logging.info("Starting the Kafka consumer")
    try:
        conn, cur = get_db_connection()
        postgres_consumer = KafkaConsumer(
            'Customers', 'Sales', 'Employee', 'Sales_Territory',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='events-streams',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            session_timeout_ms=60000,  
            heartbeat_interval_ms=20000,  
            max_poll_interval_ms=300000 
        )

        for msg in postgres_consumer:
            topic = msg.topic
            msg_val = msg.value
            #logging.info(f"Received message from topic {topic}: {msg_val}")

            try:
                if topic == 'Sales_Territory':
                    cur.execute(sales_region_load, sales_territory_data_extractor(data=msg_val))
                elif topic == 'Employee':
                    cur.execute(employee_table_load, employee_data_extractor(data=msg_val))
                elif topic == 'Customers':
                    cur.execute(customer_table_load, customer_data_extractor(data=msg_val))
                elif topic == 'Sales':
                    cur.execute(sales_table_load, sales_data_extractor(data=msg_val))
                
                conn.commit()
                #logging.info(f"Successfully loaded record from topic {topic} to Postgres")
            except Exception as e:
                logging.error(f"Error loading record from topic {topic}: {e}")
                conn.rollback()

    except Exception as e:
        logging.error(f"Error initializing Kafka consumer or connecting to database: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed")

if __name__ == '__main__':
    main()
