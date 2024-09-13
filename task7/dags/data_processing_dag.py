from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from pymongo import MongoClient
from datetime import datetime
import logging
import pandas as pd
import os

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Define paths and datasets
data_file = '/opt/airflow/data/tiktok_google_play_reviews.csv'
processed_data_file = '/opt/airflow/data/processed_file.csv'
processed_dataset = Dataset(processed_data_file)

def check_file_empty(filepath):
    if os.stat(filepath).st_size == 0:
        return 'file_is_empty'
    return 'data_processing_tasks.replace_nulls'

def process_data():
    logging.info("Starting data processing...")
    df = pd.read_csv(data_file)
    df.fillna('-', inplace=True)
    df.sort_values(by='at', inplace=True)
    df['content'] = df['content'].str.replace(r'[^\w\s,.!?]', '', regex=True)
    df.to_csv(processed_data_file, index=False)
    logging.info("Data processing completed and saved to processed_file.csv")

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

with DAG('data_processing_dag', default_args=default_args, schedule_interval=None) as dag:
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=data_file,
        poke_interval=10
    )

    check_file_task = BranchPythonOperator(
        task_id='check_if_file_empty',
        python_callable=check_file_empty,
        op_args=[data_file]
    )

    log_empty_file = BashOperator(
        task_id='file_is_empty',
        bash_command='echo "File is empty!"'
    )

    with TaskGroup('data_processing_tasks') as data_processing_tasks:
        replace_nulls = PythonOperator(
            task_id='replace_nulls',
            python_callable=process_data,
            outlets=[processed_dataset]
        )

    load_to_mongodb = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongo
    )

    wait_for_file >> check_file_task >> [log_empty_file, data_processing_tasks]
    data_processing_tasks >> load_to_mongodb