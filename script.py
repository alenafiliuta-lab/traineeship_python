import json
import psycopg2

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="root",
    host="localhost",
    port="5432",
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS students.rooms (
    room_id SERIAL PRIMARY KEY,
    room_name TEXT NOT NULL
    )
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS students.student (
    student_id SERIAL PRIMARY KEY,
    birthday DATE NOT NULL,
    student_name TEXT NOT NULL,
    room_id INTEGER,
    sex TEXT NOT NULL,
    FOREIGN KEY (room_id) REFERENCES rooms (id) ON DELETE SET NULL
)
""")

with open("C:\\Users\\info\\Desktop\\rooms.json", encoding="utf-8") as f:
    rooms_data = json.load(f)

for room in rooms_data:
    cursor.execute(
        "INSERT INTO students.rooms (room_id, room_name) VALUES (%s, %s) ON CONFLICT (room_id) DO NOTHING",
        (room["id"], room["name"]),
    )

with open("C:\\Users\\info\\Desktop\\students.json", encoding="utf-8") as f:
    students_data = json.load(f)

for student in students_data:
    cursor.execute(
        "INSERT INTO students.student (student_id, birthday, student_name, room_id, sex) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (student_id) DO NOTHING",
        (
            student["id"],
            student["birthday"],
            student["name"],
            student["room"],
            student["sex"],
        ),
    )

# CRTEAT INDEX FOR TASK 1-3:
cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_room_id ON students.rooms (room_id);
CREATE INDEX IF NOT EXISTS idx_student_room_id ON students.student (room_id)
""")

# 1) List of rooms and the number of students in each of them
cursor.execute("""
SELECT json_agg(result) AS rooms
FROM (
    SELECT r.room_name, COUNT (s.student_id) AS student_count
    FROM students.rooms r
    left join students.student s ON r.room_id=s.room_id
    GROUP BY r.room_name ORDER BY student_count DESC
) AS result;
""")

# 2) 5 rooms with the smallest average age of students
cursor.execute("""
SELECT json_agg(result) AS rooms
FROM (
    SELECT r.room_id, ROUND(AVG(EXTRACT (YEAR FROM AGE(s.birthday)))) AS age
    FROM students.rooms AS r
    JOIN students.student AS s ON r.room_id=s.room_id
    GROUP BY r.room_id
    ORDER BY age ASC
    LIMIT 5
) AS result;
 """)

# 3) 5 rooms with the largest difference in the age of students
cursor.execute("""
SELECT json_agg(result) AS rooms
FROM (
    SELECT r.room_id,  MAX (AGE(s.birthday))- MIN (AGE(s.birthday)) AS difference
    FROM students.rooms AS r
    JOIN students.student AS s ON r.room_id=s.room_id
    GROUP BY r.room_id
    ORDER BY difference DESC
    LIMIT 5
) AS result;
 """)

# CRTEAT INDEX FOR TASK 4:
cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_room_id ON students.rooms (room_id);
CREATE INDEX IF NOT EXISTS idx_student_room_id_and_sex 
ON students.student (room_id, sex)
""")

# 4) List of rooms where different-sex students live
cursor.execute("""
SELECT json_agg(result) AS rooms
FROM (
    SELECT r.room_id
    FROM students.rooms r
    JOIN students.student s1 ON r.room_id = s1.room_id
    JOIN students.student s2 ON s1.room_id = s2.room_id AND s1.sex != s2.sex
    GROUP BY r.room_id
) AS result;
 """)


conn.commit()
cursor.close()
conn.close()
