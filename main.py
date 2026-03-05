from database_conn import Database
from loader import DataLoader
from query import QueryExecutor
import json
import argparse


def main():
    db = Database(
        dbname="postgres",
        user="postgres",
        password="root",
        host="localhost",
        port="5432",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("path_to_rooms", type=str, help="путь к файлу rooms.json")
    parser.add_argument("path_to_students", type=str, help="путь к файлу students.json")
    parser.add_argument(
        "format_output", type=str, help="выходной формат данных: xml или json?"
    )
    args = parser.parse_args()

    loader = DataLoader(db)
    loader.create_tables()
    loader.load_rooms(args.path_to_rooms)
    loader.load_students(args.path_to_students)

    query = QueryExecutor(db)
    result1 = query.get_room_student_counts()
    result2 = query.get_rooms_smallest_avg_age()
    result3 = query.get_rooms_largest_age_difference()
    result4 = query.get_rooms_with_different_sex_students()
    data = {
        "room_student_counts": result1,
        "rooms_smallest_avg_age": result2,
        "rooms_largest_age_difference": result3,
        "rooms_with_different_sex_students": result4,
    }

    if args.format_output == "json":
        with open("output.json", "w") as f:
            json.dump(
                data, f, indent=4
            )  # Записываем в файл output.json в директорию проекта
        print("Данные успешно сохранены в файле output.json")

    elif args.format_output == "xml":
        with open("output.xml", "w") as f:
            f.write("<laba><task_1>\n<get_room_student_counts>\n")
            f.write(str(result1))
            f.write("\n</get_room_student_counts>\n</task_1>")

            f.write("<task_2>\n<get_rooms_smallest_avg_age>\n")
            f.write(str(result2))
            f.write("\n</get_rooms_smallest_avg_age>\n</task_2>")

            f.write("<task_3>\n<get_rooms_largest_age_difference>\n")
            f.write(str(result3))
            f.write("\n</get_rooms_largest_age_difference>\n</task_3>")

            f.write("<task_4>\n<get_rooms_with_different_sex_students>\n")
            f.write(str(result4))
            f.write("\n</get_rooms_with_different_sex_students>\n</task_4>\n</laba>")
        print("Данные успешно сохранены в файле output.xml")
    elif args.format_output not in ("json", "xml"):
        raise ValueError("Укажите правильный формат: json или xml.")

    db.close()


if __name__ == "__main__":
    main()
