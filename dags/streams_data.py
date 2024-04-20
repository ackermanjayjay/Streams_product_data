import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from sqlalchemy import create_engine


default_args = {"owner": "mad", "retry": 5, "retry_delay": timedelta(minutes=5)}


def getData():
    response_API = requests.get("https://dummyjson.com/products")
    print(response_API.status_code)
    data = response_API.text
    data_load_json = json.loads(data)
    return data_load_json


def stream_data():
    id = []
    title = []
    description = []
    price = []
    discountPercentage = []
    rating = []
    stock = []
    brand = []
    category = []
    res = getData()
    for values in res["products"]:
        id.append(values["id"])
        title.append(values["title"])
        description.append(values["description"])
        price.append(values["price"])
        discountPercentage.append(values["discountPercentage"])
        rating.append(values["rating"])
        stock.append(values["stock"])
        brand.append(values["brand"])
        category.append(values["brand"])
    df = {
        "id": id,
        "title": title,
        "description": description,
        "price": price,
        "discount": discountPercentage,
        "rating": rating,
        "stock": stock,
        "brand": brand,
        "category": category,
    }
    return df


def to_df():
    res = stream_data()
    return pd.DataFrame(res)

def insert_data():
    import psycopg2
    import time

    res = to_df()
    # # Example: 'postgresql://username:password@localhost:5432/your_database'
    # code from https://medium.com/@askintamanli/fastest-methods-to-bulk-insert-a-pandas-dataframe-into-postgresql-2aa2ab6d2b24
    engine = create_engine(
        "postgresql://airflow:airflow@host.docker.internal/productjson"
    )

    start_time = time.time()  # get start time before insert
    # Create a connection to your PostgreSQL database
    conn = psycopg2.connect(
        database="productjson",
        user="airflow",
        password="airflow",
        host="host.docker.internal",
        port="5432",
    )
    try:
        res.to_sql(name="products", con=engine, if_exists="replace")
        conn.commit()
        end_time = time.time()  # get end time after insert
        total_time = end_time - start_time  # calculate the time
        logging.info(f"Insert time: {total_time} seconds")

    except Exception as e:
        logging.error(f"could not insert data due to {e}")


with DAG(
    default_args=default_args,
    dag_id="dag_data_product_etl_to_table_v03",
    start_date=datetime(2024, 4, 1),
    schedule_interval="@once",
) as dag:
    task1 = PythonOperator(task_id="get_data", python_callable=stream_data)
    task2 = PostgresOperator(
        task_id="create_product_table",
        postgres_conn_id="product_json",
        sql="""
            CREATE TABLE IF NOT EXISTS products (
            id  VARCHAR  PRIMARY KEY,
            title VARCHAR NOT NULL,
            price INT NOT NULL,
            discount FLOAT NOT NULL,
            stock INT NOT NULL,
            brand VARCHAR NOT NULL,
            category VARCHAR NOT NULL);
          """,
    )
    task3 = PythonOperator(
        task_id="insert_to_table",
        python_callable=insert_data
    )
    # task1 >> task2 >> task3
    task1 >> task3
