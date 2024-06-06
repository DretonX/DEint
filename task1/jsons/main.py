from config import setup_logging
from database import connect_to_database
from data_loader import load_data


def main():
    logger = setup_logging()
    logger.info("Main function started")

    conn = connect_to_database()
    cursor = conn.cursor()
    try:
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

        load_data(cursor, 'jsons/rooms.json', 'rooms')
        load_data(cursor, 'jsons/students.json', 'students')

        conn.commit()
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
