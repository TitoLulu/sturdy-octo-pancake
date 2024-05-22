import json
import psycopg2
import os 

from kafka import KafkaConsumer
from db_connect import get_db_connection

def load_customer_data(cur, conn, data):
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
def load_sales_data(cur, conn, data):
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
def main():
    conn, cur = get_db_connection()
    consumer = KafkaConsumer(
        'Customers', 'Sales','rembo.public.sales',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for msg in consumer:
        topic = msg.topic
        msg_val = msg.value

        if topic == 'Customers': 
            load_customer_data(cur=cur, conn=conn,data=msg_val)
        elif topic == 'Sales':
            load_sales_data(cur=cur, conn=conn, data=msg_val)
        elif topic == 'rembo.public.sales':
            print(topic)
    conn.close()

if __name__ == '__main__':
    main()
