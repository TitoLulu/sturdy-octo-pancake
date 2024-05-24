customer_table_load = """INSERT INTO customer 
    (first_name, last_name, gender, birth_date, age, email_address, english_occupation, address_line1, customer_key)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

sales_table_load = """INSERT INTO sales 
    (order_date, customerkey, sales_amount, employee_id, unit_price)
    VALUES (%s, %s, %s, %s, %s)
"""

employee_table_load = """INSERT INTO employee 
    (employee_name, employee_territory_region)
    VALUES (%s, %s)
"""

sales_region_load = """INSERT INTO sales_territory 
    (sales_territory_country, sales_territory_city, sales_territory_region)
    VALUES (%s, %s, %s)
"""
