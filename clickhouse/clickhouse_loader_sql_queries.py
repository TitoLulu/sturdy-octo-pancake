customer_table_load = """INSERT INTO Customer 
    (Customer_Id,First_Name, Last_Name, Gender, Birth_Date, Email_Address, English_Occupation, Address_Line1, Customer_Key, Phone)
    VALUES
    """

sales_table_load = """INSERT INTO Sales 
    (Sales_Id, Order_Date, CustomerKey, Sales_Amount, Employee_Id, Unit_Price)
    VALUES
    """

employee_table_load = """INSERT INTO Employee
    (Employee_Id, Employee_Name, Employee_Territory_Region)
    VALUES 
    """
sales_territory_table_load ="""INSERT INTO Sales_Territory 
    (Sales_Territory_Id, Sales_Territory_Country, Sales_Territory_Region, Sales_Territory_City)
    VALUES 
    """