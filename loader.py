import json
import psycopg2
import argparse
from database_conn import Database


class DataLoader:
    def __init__(self, db: Database):
        self.db = db

    def create_tables(self):
        with self.db.connection.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS students.rooms (
                    room_id SERIAL PRIMARY KEY,
                    room_name TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS students.student (
                    student_id SERIAL PRIMARY KEY,
                    birthday DATE NOT NULL,
                    student_name TEXT NOT NULL,
                    room_id INTEGER,
                    sex TEXT NOT NULL,
                    FOREIGN KEY (room_id) REFERENCES students.rooms (room_id) ON DELETE SET NULL
                )
            """)
            self.db.connection.commit()

    def load_rooms(self, path_to_rooms):
        # Загружаем данные о комнатах из JSON-файла
        with open(args.path_to_rooms, "r", encoding="utf-8") as f:
            rooms = json.load(f)
        with self.db.connection.cursor() as cur:
            for room in rooms:
                cur.execute(
                    """
                    INSERT INTO students.rooms (room_id, room_name)
                    VALUES (%s, %s)
                    ON CONFLICT (room_id) DO NOTHING
                """,
                    (room["id"], room["name"]),
                )
            self.db.connection.commit()

    def load_students(self, path_to_students):
        # Загружаем данные о cтудентах из JSON-файла
        with open(args.path_to_students, "r", encoding="utf-8") as f:
            students = json.load(f)
        with self.db.connection.cursor() as cur:
            for student in students:
                cur.execute(
                    """
                    INSERT INTO students.student (student_id, birthday, student_name, room_id, sex)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (student_id) DO NOTHING
                """,
                    (
                        student["id"],
                        student["birthday"],
                        student["name"],
                        student["room"],
                        student["sex"],
                    ),
                )
            self.db.connection.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path_to_rooms", type=str, help="путь к файлу rooms.json")
    parser.add_argument("path_to_students", type=str, help="путь к файлу students.json")
    parser.add_argument(
        "format_output", type=str, help="выходной формат данных: xml или json?"
    )
    args = parser.parse_args()
    print("path_to_rooms:", args.path_to_rooms)
    print("path_to_students:", args.path_to_students)

    # Соединение с базой данных
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="root",
        host="localhost",
        port="5432",
    )
    cursor = conn.cursor()

    conn.commit()
    cursor.close()
    conn.close()
