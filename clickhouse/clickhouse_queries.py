
customer_table_drop = "DROP TABLE IF EXISTS Customer;"
employee_table_drop = "DROP TABLE IF EXISTS Employee;"
sales_territory_table_drop = "DROP TABLE IF EXISTS Sales_Territory;"
sales_table_drop = "DROP TABLE IF EXISTS Sales;"


customer_table_create = ("""
    CREATE TABLE Customer (
        Customer_Id UInt32,
        Last_Name String,
        Address_Line1 String,
        Birth_Date String,
        Customer_Key String,
        Email_Address String,
        English_Occupation String,
        First_Name String,
        Gender String,
        Phone String,
        PRIMARY KEY (Customer_Id)
    ) ENGINE = MergeTree()
    ORDER BY Customer_Id;
""")


sales_territory_table_create = ("""
    CREATE TABLE Sales_Territory (
        Sales_Territory_Id UInt32,
        Sales_Territory_Country String,
        Sales_Territory_Region String,
        Sales_Territory_City String,
        PRIMARY KEY (Sales_Territory_Id)
    ) ENGINE = MergeTree()
    ORDER BY Sales_Territory_Id;
""")

employee_table_create = ("""
    CREATE TABLE Employee (
        Employee_Id UInt32,
        Employee_Name String,
        Employee_Territory_Region String,
        PRIMARY KEY (Employee_Id)
    ) ENGINE = MergeTree()
    ORDER BY Employee_Id;
""")

sales_table_create = ("""
    CREATE TABLE Sales (
        Sales_Id UInt32,
        CustomerKey String,
        Order_Date String,
        Sales_Amount String,
        Unit_Price String,
        Employee_Id Int32,
        PRIMARY KEY (Sales_Id)
    ) ENGINE = MergeTree()
    ORDER BY Sales_Id;
""")
click_drop_table_queries = [customer_table_drop, employee_table_drop, sales_territory_table_drop, sales_table_drop]
click_create_table_queries = [customer_table_create, employee_table_create, sales_territory_table_create, sales_table_create]