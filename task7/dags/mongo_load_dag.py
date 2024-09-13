from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from pymongo import MongoClient
from datetime import datetime
import logging
import pandas as pd

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Define paths and datasets
processed_data_file = '/opt/airflow/data/processed_file.csv'
processed_dataset = Dataset(processed_data_file)

def load_to_mongo():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['airflow_db']
    collection = db['processed_data']
    df = pd.read_csv(processed_data_file)
    collection.insert_many(df.to_dict('records'))
    logging.info("Data successfully loaded into MongoDB.")

default_args = {
    'start_date': datetime(2023, 1, 1),
}

with DAG('mongo_load_dag', default_args=default_args, schedule=[processed_dataset]) as dag:
    load_to_mongodb = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongo
    )

    load_to_mongodb