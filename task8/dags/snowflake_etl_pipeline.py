from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Задаем аргументы DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Создаем DAG
with DAG(
        'snowflake_etl_pipeline',
        default_args=default_args,
        description='ETL pipeline to load data into Snowflake',
        schedule_interval=None,
        template_searchpath=['/opt/airflow/sql'],
        catchup=False,
) as dag:
    # create raw data table
    create_raw_table = SnowflakeOperator(
        task_id='create_raw_table',
        sql='create_tables.sql',
        snowflake_conn_id='snowflake_conn',
        dag=dag,
    )

    # load data to the table
    load_raw_data = SnowflakeOperator(
        task_id='load_raw_data',
        sql='load_raw_data.sql',
        snowflake_conn_id='snowflake_conn',
        dag=dag,
    )

    # create transform tables
    create_transformed_tables = SnowflakeOperator(
        task_id='create_transformed_tables',
        sql='create_transformed_tables.sql',
        snowflake_conn_id='snowflake_conn',
        dag=dag,
    )

    # Logining of loading
    log_affected_rows = SnowflakeOperator(
        task_id='log_affected_rows',
        sql='log_affected_rows.sql',
        snowflake_conn_id='snowflake_conn',
        dag=dag,
    )


    create_raw_table >> load_raw_data >> create_transformed_tables >> log_affected_rows

