import json
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

    def read_json_file(self, file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_rooms(self, path_to_rooms):
        rooms = self.read_json_file(path_to_rooms)
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
        students = self.read_json_file(path_to_students)
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
