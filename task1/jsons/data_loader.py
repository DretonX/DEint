import json
import logging


def load_data(cursor, file_path, table_name):
    logger = logging.getLogger()
    logger.info(f"Starting to load data into {table_name} from {file_path}")
    with open(file_path, 'r') as file:
        data = json.load(file)
    for entry in data:
        if table_name == 'rooms':
            cursor.execute("INSERT INTO rooms (id, name) VALUES (%s, %s)", (entry['id'], entry['name']))
        elif table_name == 'students':
            cursor.execute("INSERT INTO students (id, name, birthday, gender, room_id) VALUES (%s, %s, %s, %s, %s)",
                           (entry['id'], entry['name'], entry['birthday'], entry['sex'], entry['room']))
