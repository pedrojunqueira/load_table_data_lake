from datetime import datetime
import configparser
import json

import adlfs
from faker import Faker
import pandas as pd

config = configparser.ConfigParser()


config.read('config.ini')

CONTAINER =  config["azure"]["azure_storage_container_name"]
STORAGE_ACCOUNT_NAME = config["azure"]["azure_storage_account_name"]
STORAGE_ACCOUNT_KEY = config["azure"]["azure_storage_account_key"] 

def update_last_id(last_id:int,id_key:str)-> None:
    data = get_last_id()
    data[id_key] = last_id
    with open('last_id.json', 'w') as f:
        json.dump(data, f)

def get_last_id()-> int:
    with open('last_id.json', 'r') as f:
        data = json.load(f)
        return data

def create_records_customers(number: int):
    fake = Faker()
    records = []
    last_ids = get_last_id()
    last_id = last_ids["last_id_customer"]
    for i in range(last_id, last_id + number):
        name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()
        city = fake.city()
        records.append((i+1, name, last_name, email, city))
    update_last_id(i+1,"last_id_customer")
    return records

import pandas as pd
from faker import Faker
import random

def create_records_sales(number:int):
    fake = Faker()
    records = []
    last_ids = get_last_id()
    last_id = last_ids["last_id_sales"]
    for i in range(last_id, last_id + number):
        first_name = fake.first_name()
        last_name = fake.last_name()
        product_name = fake.word().capitalize()
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10, 100), 2)
        total_price = quantity * unit_price
        date = fake.date_this_year()
        _id = i

        record = {
            'ID': _id,
            'FirstName': first_name,
            'LastName': last_name,
            'ProductName': product_name,
            'Quantity': quantity,
            'UnitPrice': unit_price,
            'TotalPrice': total_price,
            'Date': date
        }
        records.append(record)
    update_last_id(i+1,"last_id_sales")

    return records



def load_customer_records(data: list)->None:
    columns = ['id', 'first_name', 'last_name', 'email', 'city']
    df = pd.DataFrame(data, columns=columns)
    utc_now = datetime.utcnow()
    timestamp_string = utc_now.strftime("%Y%m%d_%H%M%S")

    fs = adlfs.AzureBlobFileSystem(
        account_name=STORAGE_ACCOUNT_NAME,
        account_key=STORAGE_ACCOUNT_KEY,
    )

    # Save the CSV file to ADLS
    with fs.open(f"{CONTAINER}/{'customers'}/{timestamp_string}-customers.csv", "wb") as f:
        df.to_csv(f, index=False)

def load_sales_records(data: list)->None:
    df = pd.DataFrame(data)
    utc_now = datetime.utcnow()
    timestamp_string = utc_now.strftime("%Y%m%d_%H%M%S")

    fs = adlfs.AzureBlobFileSystem(
        account_name=STORAGE_ACCOUNT_NAME,
        account_key=STORAGE_ACCOUNT_KEY,
    )

    # Save the CSV file to ADLS
    with fs.open(f"{CONTAINER}/{'sales'}/{timestamp_string}-sales.csv", "wb") as f:
        df.to_csv(f, index=False)

r = create_records_sales(200)

load_sales_records(r)





