/* Schemas DDL for Rembo */

/* ClickHouse Database (Source) */
CREATE DATABASE rembo;

USE rembo;

/* Customer table */
CREATE TABLE Customer (
    Customer_Id Int32,
    Last_Name String,
    Address_Line1 String,
    Address_Line2 String,
    Birth_Date String,
    Age String,
    Commute_Distance String,
    Customer_Alternate_Key String,
    Customer_Key String,
    Date_First_Purchase String,
    Email_Address String,
    English_Education String,
    English_Occupation String,
    French_Education String,
    First_Name String,
    French_Occupation String,
    Gender String,
    House_Owner_Flag String,
    Marital_Status String,
    Middle_Name String,
    Name_Style String,
    Number_Cars_Owned String,
    Number_Children_At_Home String,
    Phone String,
    Spanish_Education String,
    Spanish_Occupation String,
    Suffix String,
    Title String,
    Total_Children String,
    Yearly_Income String,
    PRIMARY KEY (Customer_Id)
) ENGINE = MergeTree()
ORDER BY Customer_Id;

/* Sales_Territory table */
CREATE TABLE Sales_Territory (
    Sales_Territory_Id Int32,
    Sales_Territory_Country String,
    Sales_Territory_Region String,
    Sales_Territory_City String,
    PRIMARY KEY (Sales_Territory_Id)
) ENGINE = MergeTree()
ORDER BY Sales_Territory_Id;

/* Employee table */
CREATE TABLE Employee (
    Employee_Id Int32,
    Employee_Name String,
    Employee_Territory_Region String,
    PRIMARY KEY (Employee_Id)
) ENGINE = MergeTree()
ORDER BY Employee_Id;

/* Sales table */
CREATE TABLE Sales (
    Sales_Id Int32,
    CurrencyKey String,
    CustomerKey String,
    Discount_Amount String,
    DueDate String,
    DueDateKey String,
    Extended_Amount String,
    Freight String,
    Order_Date String,
    Order_Quantity String,
    Product_Standard_Cost String,
    Revision_Number String,
    Sales_Amount String,
    Sales_Order_Line_Number String,
    Sales_Order_Number String,
    SalesTerritoryKey String,
    ShipDate String,
    Tax_Amt String,
    Total_Product_Cost String,
    Unit_Price String,
    Unit_Price_Discount_Pct String,
    Employee_Id Int32,
    PRIMARY KEY (Sales_Id)
) ENGINE = MergeTree()
ORDER BY Sales_Id;
