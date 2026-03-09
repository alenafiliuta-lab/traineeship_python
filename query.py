class QueryExecutor:
    def __init__(self, db):
        self.db = db

    def create_indexes(self):
        return self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_room_id ON students.rooms (room_id);
            CREATE INDEX IF NOT EXISTS idx_student_room_id ON students.student (room_id)
        """)

    def get_room_student_counts(self):
        self.db.execute("""
            SELECT r.room_name, COUNT (s.student_id) AS student_count
            FROM students.rooms r
            left join students.student s ON r.room_id=s.room_id
            GROUP BY r.room_name ORDER BY student_count DESC;
        """)
        return self.db.fetchall()

    def get_rooms_smallest_avg_age(self):
        self.db.execute("""
            SELECT r.room_id, ROUND(AVG(EXTRACT (YEAR FROM AGE(s.birthday))))::integer AS age
            FROM students.rooms AS r
            JOIN students.student AS s ON r.room_id=s.room_id
            GROUP BY r.room_id
            ORDER BY age ASC
            LIMIT 5
        """)
        return self.db.fetchall()

    def get_rooms_largest_age_difference(self):
        self.db.execute("""
            SELECT r.room_id,  EXTRACT(YEAR FROM (MAX(AGE(s.birthday)) - MIN(AGE(s.birthday))))::integer AS difference
            FROM students.rooms AS r
            JOIN students.student AS s ON r.room_id=s.room_id
            GROUP BY r.room_id
            ORDER BY difference DESC
            LIMIT 5
        """)
        return self.db.fetchall()

    def create_indexes_for_task_4(self):
        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_student_room_id_and_sex
            ON students.student (room_id, sex)
        """)
        return self.db.fetchall()

    def get_rooms_with_different_sex_students(self):
        self.db.execute("""
            SELECT r.room_id
            FROM students.rooms r
            JOIN students.student s1 ON r.room_id = s1.room_id
            JOIN students.student s2 ON s1.room_id = s2.room_id AND s1.sex != s2.sex
            GROUP BY r.room_id
        """)
        return self.db.fetchall()
