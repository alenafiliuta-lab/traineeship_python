import psycopg2


class Database:
    def __init__(self, dbname, user, password, host, port):
        self.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        self.cursor = self.connection.cursor()

    def execute(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


if __name__ == "__main__":
    # Этот код выполнится только при прямом запуске файла
    print("Тестирование класса Database")

    try:
        # Создание экземпляра класса
        db = Database(
            dbname="postgres",
            user="postgres",
            password="root",
            host="localhost",
            port="5432",
        )
        print("✓ Подключение к БД успешно")

        # # Тестовый запрос
        # db.execute("SELECT version();")
        # version = db.fetchall()
        # print(f"✓ Версия PostgreSQL: {version[0][0]}")

        db.close()
        print("✓ Соединение закрыто")

    except Exception as e:
        print(f"✗ Ошибка: {e}")
#
#
#
