import json
import psycopg2
import os 

from kafka import KafkaConsumer
from clickhouse.clickhouse_db_connection import get_clickhouse_client

def escape_string(value):
    """Escapes single quotes in a string for safe SQL insertion."""
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"

def load_customer_data(client, data):
    sql = """INSERT INTO Customer 
    (Customer_Id,First_Name, Last_Name, Gender, Birth_Date, Email_Address, English_Occupation, Address_Line1, Customer_Key, Phone)
    VALUES
    """
    customer_data = data["payload"]["after"]
    phone_number = customer_data.get("phone") or '0123'
    values = (
        customer_data["customer_id"],
        customer_data["first_name"],
        customer_data["last_name"],
        customer_data["gender"],
        customer_data["birth_date"],
        customer_data["email_address"],
        customer_data["english_occupation"],
        customer_data["address_line1"],
        customer_data["customer_key"],
        phone_number,  # Handle potential missing phone number
    )
    formatted_values = ", ".join(escape_string(value) if isinstance(value, str) else str(value) for value in values)

    try:
        client.command(f"{sql} ({formatted_values})")
    except Exception as e:
        print(f"fulfillment.public.customer cdc error encountered: {e}") 
        

def load_sales_data(client, data):
    sql = """INSTERT INTO Sales 
    (Sales_Id,Order_Date, CustomerKey, Sales_Amount, Employee_Id, Unit_Price)
    VALUES
    """
    values = (
        data["payload"]["after"]["sales_id"],
        data["payload"]["after"]["order_date"],
        data["payload"]["after"]["customerkey"],
        data["payload"]["after"]["sales_amount"],
        data["payload"]["after"]["employee_id"],
        data["payload"]["after"]["unit_price"],
    )
    formatted_values = ", ".join(escape_string(value) if isinstance(value, str) else str(value) for value in values)
    try:
        client.command(f"{sql} ({formatted_values})")
    except Exception as e:
        print(f"fulfillment.public.sales cdc error encountered: {e}")


def load_employee_data(client, data):
    sql = """ INSERT INTO Employee
    (Employee_Id, Employee_Name, Employee_Territory_Region)
    VALUES 
    """

    values = (
        data["payload"]["after"]["employee_id"],
        data["payload"]["after"]["employee_name"],
        data["payload"]["after"]["employee_territory_region"],

    )
    formatted_values = ", ".join(escape_string(value) if isinstance(value, str) else str(value) for value in values)
    try:
        client.command(f"{sql} ({formatted_values})")
    except Exception as e:
        print(f"fulfillment.public.employee cdc error encountered: {e}")

def load_sale_territory_data(client, data):
    sql = """INSERT INTO Slales_Territory 
    (Sales_Territory_Id, Sales_Territory_Country, Sales_Territory_Region, Sales_Territory_City)
    VALUES 
    """

    values = (
        data["payload"]["after"]["sales_territory_id"],
        data["payload"]["after"]["sales_territory_country"],
        data["payload"]["after"]["sales_territory_region"],
        data["payload"]["after"]["sales_territory_city"],
    )
    formatted_values = ", ".join(escape_string(value) if isinstance(value, str) else str(value) for value in values)
    try:
        client.command(f"{sql} ({formatted_values})")
    except Exception as e:
        print(f"fulfillment.public.sales_territory cdc error encountered: {e}")

def main():
    client = get_clickhouse_client()

    dbz_consumer = KafkaConsumer(
        'fulfillment.public.customer','fulfillment.public.sales','fulfillment.public.employee','fulfillment.public.sales_territory',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='debezium-streams',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    try:
        for msg in dbz_consumer:
            topic = msg.topic
            msg_val = msg.value
            
            if topic == 'fulfillment.public.employee':
                load_employee_data(client=client, data=msg_val)
            if topic == 'fulfillment.public.sales_territory':
                load_sale_territory_data(client=client, data=msg_val)
            if topic == 'fulfillment.public.customer':
                load_customer_data(client=client, data=msg_val) 
            if topic == 'fulfillment.public.sales':
                load_sales_data(client=client, data=msg_val)
    except Exception as e:
        print(f"Stopping CDC consumer... {e}")
        dbz_consumer.close()
    finally:
        client.disconnect()
    



if __name__ == '__main__':
    main()
