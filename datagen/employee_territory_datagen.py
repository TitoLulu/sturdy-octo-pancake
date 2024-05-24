import random 
import requests
import logging
import json 

from Countrydetails import countries
from datagen.customer_sales_datagen import generate_data
from postgres.postgres_db_connection import get_db_connection
from clickhouse.clickhouse_db_connection import get_clickhouse_client
from uuid import uuid4
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)


def generate_employee_data(res):
    regions = "regions.txt"
    with open(regions) as f:
        data = list(set(f.read().splitlines()))[:11] 

    employees = {}
    employees["employee_name"] = res["name"]["first"] + " " + res["name"]["last"]
    employees["employee_territory_region"] = random.choice(data)
    return employees


def generate_territories_data():
    regions = []
    data = countries.all_countries()
    countries_and_capitals = data.capitals()
    countries_and_regions = data.regions()
    for key, val in countries_and_capitals.items():
        for entry in countries_and_regions:
            if key == entry["country"]:
                region_data = {
                    "sales_territory_id": uuid4().hex,
                    "sales_territory_country": key,
                    "sales_territory_city": val,
                    "sales_territory_region": entry["location"]
                }
                regions.append(region_data)
    return regions


def main():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(3, 1, 2), max_block_ms=5000) 
    employee_counter = 100
    #employees = []
    while employee_counter > 0:
        res = generate_data()
        data = generate_employee_data(res=res)
        #employees.append(data)
        producer.send('Employee', json.dumps(data,default=str).encode('utf-8'))
        employee_counter -= 1

    # Generate and insert territory data
    territories = generate_territories_data()
    for territory in territories:
        producer.send('Sales_Territory', json.dumps(territory,default=str).encode('utf-8'))
  
 

if __name__ == '__main__':
    main()
