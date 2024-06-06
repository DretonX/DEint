import psycopg2
import os
import time
import logging


def connect_to_database(retries=5):
    dbname = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')

    for i in range(retries):
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            return conn
        except psycopg2.OperationalError as e:
            logger = logging.getLogger()
            logger.error(f"Attempt {i + 1} of {retries} failed: {e}")
            time.sleep(5)
    raise Exception("All attempts to connect to the database failed.")
