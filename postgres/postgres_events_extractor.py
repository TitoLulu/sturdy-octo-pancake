def customer_data_extractor(data):
    return (data.get("first_name"),
        data.get("last_name"),
        data.get("gender"),
        data.get("birth_date"),
        data.get("age"),
        data.get("email_address"),
        data.get("english_occupation"),
        data.get("address_line1"),
        data.get("customer_key")
    )

def employee_data_extractor (data):
        
    return (data.get("employee_name"),
        data.get("employee_territory_region")
    )

def sales_territory_data_extractor(data):
    return (
            data.get("sales_territory_country"),
            data.get("sales_territory_city"),
            data.get("sales_territory_region")
    )

def sales_data_extractor(data): 
    return (
        data.get("order_date"),
        data.get("customer_key"),
        data.get("sales_amount"),
        data.get("employee_id"),
        data.get("unit_price")
)


