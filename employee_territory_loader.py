import random 
import requests

from Countrydetails import countries
from datagen import generate_data
from db_connection import get_db_connection
from uuid import uuid4



def generate_employee_data(res):
    regions = "regions.txt"
    with open(regions) as f:
        data = list(set(f.read().splitlines()))[:11] 

    employees = {}
    employees["employee_name"] = res["name"]["first"] + " " + res["name"]["last"]
    employees["employee_territory_region"] = random.choice(data)
    return employees

def load_employee_data(cur, conn, data):
    for entry in data:
        sql = """
        INSERT INTO employee (employee_name, employee_territory_region)
        VALUES (%s, %s)
        """
        cur.execute(sql, (
            entry["employee_name"],
            entry["employee_territory_region"]
        ))
        conn.commit()
  
   

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

def insert_regions_data(cur,conn, regions):
    for entry in regions:
        sql = """
        INSERT INTO sales_territory 
        (sales_territory_country, sales_territory_city, sales_territory_region) 
        VALUES (%s, %s, %s)
        """
        cur.execute(sql, (
            entry["sales_territory_country"],
            entry["sales_territory_city"],
            entry["sales_territory_region"]
        ))
        conn.commit()
    

def main():
    conn,cur = get_db_connection()
    employee_counter = 100
    employees = []
    while employee_counter > 0:
        res = generate_data()
        data = generate_employee_data(res=res)
        employees.append(data)
        employee_counter -= 1
    load_employee_data(cur=cur, conn=conn,data=employees)

    # Generate and insert territory data
    territories = generate_territories_data()
    insert_regions_data(cur=cur, conn=conn,regions=territories)
    conn.close()
 

if __name__ == '__main__':
    main()
