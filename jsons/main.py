import os
import psycopg2
import json
import time
from typing import Any


def load_data(cursor: Any, file_path: str, table_name: str) -> None:
    """
    Loads data from a JSON file into the specified database (PostgreSQL) table.

    cursor - The cursor object to execute database queries.
    file_path - The path to the JSON file containing data.
    table_name - The name of the table to insert data into.

    """
    with open(file_path, 'r') as file:
        data = json.load(file)
        if table_name == 'rooms':
            for entry in data:
                cursor.execute("INSERT INTO rooms (id, name) VALUES (%s, %s)", (entry['id'], entry['name']))
        elif table_name == 'students':
            for entry in data:
                cursor.execute("INSERT INTO students (id, name, birthday, gender, room_id) VALUES (%s, %s, %s, %s, %s)",
                               (entry['id'], entry['name'], entry['birthday'], entry['sex'], entry['room']))


def main() -> None:
    """
    Main function to connect to the PostgreSQL database, create tables (if they don't exist),
    and load data from JSON files into the tables (rooms, students).

    """
    # Connection parameters from environment variables
    dbname = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')

    # Trys of connection attempts
    retries = 5
    for i in range(retries):
        try:
            # Connect to the PostgreSQL database
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            cursor = conn.cursor()

            # Test of SQL query
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()
            print(f"Connected to PostgreSQL database. Version: {db_version[0]}")

            # Create tables (if they don't exist)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rooms (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS students (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    birthday DATE NOT NULL,
                    gender VARCHAR(10) NOT NULL,
                    room_id INT,
                    FOREIGN KEY (room_id) REFERENCES rooms (id)
                );
            """)

            # Load data from JSON files (rooms, students)
            load_data(cursor, 'jsons/rooms.json', 'rooms')
            load_data(cursor, 'jsons/students.json', 'students')

            # Commit the changes
            conn.commit()

            cursor.close()
            conn.close()
            break
        except psycopg2.OperationalError as e:
            print(f"Attempt {i + 1} of {retries} failed: {e}")
            time.sleep(5)
    else:
        print("All attempts to connect to the database failed.")


if __name__ == '__main__':
    main()
