import json
import psycopg2
import os 
import concurrent.futures

from kafka import KafkaConsumer
from postgres.postgres_db_connection import get_db_connection

def load_customer_data(data):
    conn, cur = get_db_connection()
    sql = """INSERT INTO customer 
    (first_name, last_name, gender, birth_date, age, email_address, english_occupation, address_line1, customer_key)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur.execute(sql, (
        data.get("first_name"),
        data.get("last_name"),
        data.get("gender"),
        data.get("birth_date"),
        data.get("age"),
        data.get("email_address"),
        data.get("english_occupation"),
        data.get("address_line1"),
        data.get("customer_key")
    ))
    conn.commit()
    cur.close()
    conn.close()

def load_sales_data(data):
    conn, cur = get_db_connection()
    sql = """INSERT INTO sales 
    (order_date, customerkey, sales_amount, employee_id, unit_price)
    VALUES (%s,%s,%s,%s,%s)
    """
    cur.execute(sql, (
        data.get("order_date"),
        data.get("customer_key"),
        data.get("sales_amount"),
        data.get("employee_id"),
        data.get("unit_price")
    ))
    conn.commit()
    cur.close()
    conn.close()

def load_employee_data(data):
    conn, cur = get_db_connection()
    sql = """
        INSERT INTO employee (employee_name, employee_territory_region)
        VALUES (%s, %s)
    """
    cur.execute(sql,
        data.get("employee_name"),
        data.get("employee_territory_region")
    )
    conn.commit()
    cur.close()
    conn.close()

def load_sales_regions_data(data):
    conn, cur = get_db_connection()
    sql = """
        INSERT INTO sales_territory 
        (sales_territory_country, sales_territory_city, sales_territory_region) 
        VALUES (%s, %s, %s)
    """
    cur.execute(sql,
            data.get("sales_territory_country"),
            data.get("sales_territory_city"),
            data.get("sales_territory_region")
    )
    conn.commit()
    cur.close()
    conn.close()

def process_message(topic, msg_val):
    if topic == 'Customers': 
        load_customer_data(data=msg_val)
    elif topic == 'Sales':
        load_sales_data(data=msg_val)
    elif topic == 'Employee':
        load_employee_data(data=msg_val)
    elif topic == 'Sales_Territory':
        load_sales_regions_data(data=msg_val)

def main():
    postgres_consumer = KafkaConsumer(
        'Customers', 'Sales','Employee','Sales_Territory',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='events-streams',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for msg in postgres_consumer:
            topic = msg.topic
            msg_val = msg.value
            executor.submit(process_message, topic, msg_val)

       

if __name__ == '__main__':
    main()
