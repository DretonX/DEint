from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from pymongo import MongoClient
from datetime import datetime, timedelta
import logging
import pandas as pd
import os

# Инициализация логирования
logging.basicConfig(level=logging.INFO)

# Глобальный словарь для хранения времени последней обработки файлов
processed_files = {}

def check_for_new_files(folder_path):
    """Проверяет наличие новых или обновленных файлов в папке"""
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    new_files = []

    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        modification_time = os.path.getmtime(file_path)

        # Если файл не обрабатывался или его время изменения изменилось
        if file not in processed_files or processed_files[file] < modification_time:
            processed_files[file] = modification_time
            new_files.append(file_path)  # Добавляем путь к файлу для обработки

    return new_files

def check_file_for_processing(**kwargs):
    """Проверяет, есть ли новые или обновленные файлы для обработки"""
    folder_path = '/opt/airflow/data'
    new_files = check_for_new_files(folder_path)

    if new_files:
        kwargs['ti'].xcom_push(key='new_files', value=new_files)
        return 'create_processing_tasks'
    else:
        return 'skip_processing'

def process_data(file_path, **kwargs):
    """Обрабатывает файл, удаляет символы и сохраняет новый файл"""
    logging.info(f"Starting data processing for file {file_path}...")

    chunk_size = 10000
    processed_file_path = file_path.replace('.csv', '_processed.csv')

    # Если файл существует, удаляем его перед началом обработки, чтобы избежать накопления данных
    if os.path.exists(processed_file_path):
        os.remove(processed_file_path)

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunk.fillna('-', inplace=True)
        chunk.sort_values(by='at', inplace=True)
        chunk['content'] = chunk['content'].str.replace(r'[^\w\s,.!?]', '', regex=True)

        # Сохраняем чанки в новый файл
        chunk.to_csv(processed_file_path, mode='a', header=not os.path.exists(processed_file_path), index=False)

    logging.info(f"Data processing completed and saved to {processed_file_path}")
    return processed_file_path

def load_to_mongo(file_path, **kwargs):
    """Загружает обработанные данные в MongoDB"""
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['airflow_db']
    collection = db['processed_data']

    logging.info(f"Loading data from {file_path} into MongoDB...")

    chunk_size = 5000  # Reduce chunk size if necessary
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Batch insert
        collection.insert_many(chunk.to_dict('records'))

    logging.info(f"Data from {file_path} successfully loaded into MongoDB.")

default_args = {
    'start_date': datetime(2024, 8, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

with DAG('file_monitoring_dag_2', default_args=default_args, schedule_interval='@hourly') as dag:

    # Sensor Task для ожидания наличия файла (используем режим reschedule для оптимизации)
    wait_for_files = FileSensor(
        task_id='wait_for_files',
        filepath='/opt/airflow/data/',
        poke_interval=3600,  # Проверка каждую 1 час
        mode='reschedule'  # Используем reschedule для освобождения ресурсов
    )

    # Branch Task для проверки наличия новых файлов
    check_file_task = BranchPythonOperator(
        task_id='check_file_for_processing',
        python_callable=check_file_for_processing,
        provide_context=True
    )

    # Task для логирования, если файлы не изменились
    skip_processing = BashOperator(
        task_id='skip_processing',
        bash_command='echo "No new or updated files found!"'
    )

    def create_processing_tasks(**kwargs):
        file_paths = kwargs['ti'].xcom_pull(task_ids='check_file_for_processing', key='new_files')
        if not file_paths:
            logging.info("No new files to process.")
            return

        with TaskGroup('data_processing_tasks') as data_processing_tasks:
            for file_path in file_paths:
                file_name = os.path.basename(file_path)

                process_task = PythonOperator(
                    task_id=f'process_{file_name}',
                    python_callable=process_data,
                    op_args=[file_path],
                    provide_context=True,
                    execution_timeout=timedelta(minutes=20)  # Adjust as needed
                )

                load_task = PythonOperator(
                    task_id=f'load_to_mongodb_{file_name}',
                    python_callable=load_to_mongo,
                    op_args=[file_path.replace('.csv', '_processed.csv')],
                    provide_context=True,
                    execution_timeout=timedelta(minutes=30)  # Increased timeout
                )

                process_task >> load_task

        return data_processing_tasks

    create_processing_tasks_task = PythonOperator(
        task_id='create_processing_tasks',
        python_callable=create_processing_tasks,
        provide_context=True
    )

    # Task Dependencies
    wait_for_files >> check_file_task
    check_file_task >> skip_processing
    check_file_task >> create_processing_tasks_task
