import requests
import json

from faker import Faker

fk = Faker()  # Initialize faker generator


def generate_data():
    res = requests.get(
        "https://randomuser.me/api"
    )  # make api call to randomuser endpoint
    res = res.json()["results"][0]

    return res


def format_data(res):
    data = {}
    data["First_Name"] = res["name"]["first"]
    data["Last_Name"] = res["name"]["last"]
    data["Gender"] = res["gender"]
    data["Birth_Date"] = res["dob"]["date"]
    data["Age"] = res["dob"]["age"]
    data["Email_Address"] = res["email"]
    data["Phone"] = res["phone"]
    data["Date_First_purchase"] = res["registered"]["date"]
    data["English_Occupation"] = fk.job()
    location = (
        str(res["location"]["street"]["number"])
        + " "
        + res["location"]["street"]["name"]
        + " "
        + res["location"]["city"]
        + " "
        + res["location"]["state"]
        + " "
        + res["location"]["country"]
    )
    data["Address_Line1"] = location

    return data


res = generate_data()
data = format_data(res=res)
print(json.dumps(data, indent=3))


customer = [
    "Customer_Id",
    "Last_Name",
    "Address_Line1",
    "Address_Line2",
    "Birth_Date",
    "Age",
    "Commute_Distance",
    "Customer_Alternate_Key",
    "Customer_Key",
    "Date_First_Purchase",
    "Email_Address",
    "English_Education",
    "English_Occupation",
    "French_Education",
    "First_Name",
    "French_Occupation",
    "Gender",
    "House_Owner_Flag",
    "Marital_Status",
    "Middle_Name",
    "Name_Style",
    "Number_Cars_Owned",
    "Number_Children_At_Home",
    "Phone",
    "Spanish_Education",
    "Spanish_Occupation",
    "Suffix",
    "Title",
    "Total_Children",
    "Yearly_Income",
]
