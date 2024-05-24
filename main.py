import os
import logging
import concurrent.futures
import create_tables
import datagen.employee_territory_datagen
import datagen.customer_sales_datagen
import consumers.events_consumer
import consumers.cdc_consumer

logging.basicConfig(level=logging.INFO)


def stream_pipeline():
    logging.info("Starting Stream Processing Pipeline")
    create_tables.main()
    logging.info("Created the necessary table schemas in postgres and clickhouse")
    
    # Concurrent Processes
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(datagen.employee_territory_datagen.main),
            executor.submit(datagen.customer_sales_datagen.main),
            executor.submit(consumers.events_consumer.main),
            executor.submit(consumers.cdc_consumer.main)
        ]
        logging.info("Streaming Simulation from Source to Sink Started")
        concurrent.futures.wait(futures)

if __name__ == '__main__':
    stream_pipeline()


