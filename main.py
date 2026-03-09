from database_conn import Database
from loader import DataLoader
from query import QueryExecutor
import json
import argparse
import xml.etree.ElementTree as ET


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
            json.dump(data, f, indent=4)
        print("Данные успешно сохранены в файле output.json")

    elif args.format_output == "xml":
        root = ET.Element("Root")

        def add_data_to_xml(data, parent, tag):
            group = ET.SubElement(parent, tag)
            for item in data:
                entry = ET.SubElement(group, "Entry")
                for element in item:
                    item_elem = ET.SubElement(entry, "Item")
                    item_elem.text = str(element)

        add_data_to_xml(result1, root, "get_room_student_counts")
        add_data_to_xml(result2, root, "get_rooms_smallest_avg_age")
        add_data_to_xml(result3, root, "get_rooms_largest_age_difference")
        add_data_to_xml(result4, root, "get_rooms_with_different_sex_students")
        tree = ET.ElementTree(root)
        tree.write("output.xml", encoding="utf-8", xml_declaration=True)
        print("Данные успешно сохранены в output.xml")
    elif args.format_output not in ("json", "xml"):
        raise ValueError("Укажите правильный формат: json или xml.")

    db.close()


if __name__ == "__main__":
    main()
