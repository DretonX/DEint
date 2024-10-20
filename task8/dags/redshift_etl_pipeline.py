from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os

# DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Function to read and split SQL file into separate queries
def split_sql_statements(file_path):
    with open(file_path, 'r') as file:
        sql_script = file.read()

    # Replace all occurrences of STRING with VARCHAR(255)
    sql_script = sql_script.replace('STRING', 'VARCHAR(255)')

    # Split queries by semicolon
    statements = sql_script.split(';')
    statements = [stmt.strip() + ';' for stmt in statements if stmt.strip()]  # Remove empty lines and add `;`

    return statements

# Create DAG
with DAG(
        'redshift_etl_pipeline1',
        default_args=default_args,
        description='ETL pipeline to load data into Redshift',
        schedule_interval=None,
        template_searchpath=['/opt/airflow/sql'],
        catchup=False,
) as dag:

    # Define paths to SQL files
    create_tables_sql_path = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), 'sql/create_tables.sql')
    transform_tables_sql_path = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), 'sql/create_transformed_tables_redshift.sql')

    # PythonOperator to execute all SQL queries from a file sequentially
    def execute_sql_statements(file_path):
        statements = split_sql_statements(file_path)

        for i, statement in enumerate(statements):
            task = RedshiftSQLOperator(
                task_id=f'execute_sql_{i}',
                sql=statement,
                redshift_conn_id='redshift_conn',
                dag=dag,
            )
            task.execute(context={})

    # Use PythonOperator to execute all SQL queries for creating raw tables
    create_raw_tables = PythonOperator(
        task_id='create_raw_tables',
        python_callable=execute_sql_statements,
        op_kwargs={'file_path': create_tables_sql_path},
    )

    # Load data to the table
    load_raw_data = RedshiftSQLOperator(
        task_id='load_raw_data',
        sql='load_raw_redshift_data.sql',
        redshift_conn_id='redshift_conn',
    )

    # Create transform tables
    create_transformed_tables = PythonOperator(
        task_id='create_transformed_tables',
        python_callable=execute_sql_statements,
        op_kwargs={'file_path': transform_tables_sql_path},
    )

    # Logging of loading
    log_affected_rows = RedshiftSQLOperator(
        task_id='log_affected_rows',
        sql='log_affected_rows.sql',
        redshift_conn_id='redshift_conn',
    )

    create_raw_tables >> load_raw_data >> create_transformed_tables >> log_affected_rows