import logging

logging.basicConfig(level=logging.INFO)


def escape_string(value):
    """Escapes single quotes in a string for safe ClickHouse insertion."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    elif value is None:
        return "NULL"
    else:
        return str(value)



def customer_data_extractor(data):
    customer_data = data
    phone_number = customer_data["phone"] or '0123'
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
    return formatted_values

def sales_data_extractor(data):
    values = (
        data["sales_id"],
        data["order_date"],
        data["customerkey"],
        data["sales_amount"],
        data["employee_id"],
        data["unit_price"],
    )
    formatted_values = ", ".join(escape_string(value) if isinstance(value, str) else str(value) for value in values)
    return formatted_values

def employee_data_extractor(data):
    
    values = (
        data["employee_id"],
        data["employee_name"],
        data["employee_territory_region"],

    )
    formatted_values = ", ".join(escape_string(value) if isinstance(value, str) else str(value) for value in values)
    # slogging.info(f"Formatted Values: {formatted_values}")
    return formatted_values

def sales_territory_data_extracor(data):
    values = (
        data["sales_territory_id"],
        data["sales_territory_country"],
        data["sales_territory_region"],
        data["sales_territory_city"],
    )
    formatted_values = ", ".join(escape_string(value) if isinstance(value, str) else str(value) for value in values)
    return formatted_values