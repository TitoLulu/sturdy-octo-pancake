import requests
import json
import os
import time
import logging
import random
import psycopg2


from faker import Faker
from kafka import KafkaProducer
from uuid import uuid4
from datetime import datetime
from Countrydetails import countries
from db_connect import get_db_connection



fk = Faker()  # Initialize faker generator


def generate_data():
    res = requests.get(
        "https://randomuser.me/api"
    )  # make api call to randomuser endpoint
    res = res.json()["results"][0]

    return res



def format_customer_data(res):
    occupations = [
        "Software Developer",
        "Data Scientist",
        "Project Manager",
        "Graphic Designer",
        "Financial Analyst",
        "Marketing Specialist",
        "Sales Manager",
        "Human Resources Manager",
        "Network Administrator",
        "Product Manager",
        "Accountant",
        "Business Analyst",
        "Customer Service Representative",
        "Electrical Engineer",
        "Mechanical Engineer",
        "Web Developer",
        "Digital Marketing Manager",
        "Operations Manager",
        "Content Writer",
        "Social Media Manager"
    ]

    data = {}
    data["first_name"] = res["name"]["first"]
    data["last_name"] = res["name"]["last"]
    data["gender"] = res["gender"]
    data["birth_date"] = res["dob"]["date"]
    data["age"] = res["dob"]["age"]
    data["email_address"] = res["email"]
    data["phone"] = res["phone"]
    data["english_occupation"] = random.choice(occupations)
    location = (
        str(res["location"]["street"]["number"])
        + " "
        + res["location"]["street"]["name"]
    
    )
    data["address_line1"] = location
    data["customer_key"] = fk.passport_number()

    return data


def generate_sale(cur, conn,  customer_data):
    sql = """SELECT * FROM employee"""
    cur.execute(sql)
    employees = cur.fetchall()
    employee = random.choice(employees)
    sales = {
        "order_date": datetime.now(),
        "customer_key": customer_data["customer_key"],
        "sales_amount": random.randint(1,20),
        "employee_id": employee[0],
        "unit_price": 200
        
    }
    
    return sales

def main():
    #print(json.dumps(data, indent=3))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(3, 1, 2), max_block_ms=5000)    
    curr_time = time.time()

    conn,cur = get_db_connection()
    

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = generate_data()
            data = format_customer_data(res=res)
            sale = generate_sale(cur=cur, conn=conn, customer_data=data)
            producer.send('Customers', json.dumps(data).encode('utf-8'))
            producer.send('Sales', json.dumps(sale,default=str).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


if __name__ == '__main__':
    main()