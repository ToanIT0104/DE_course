import os
import json
import psycopg2
from config import load_config

def insert_products(data):
    sql = """
    INSERT INTO products_tiki (id, name, url_key, price, description, image_url)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, data)
            conn.commit()
            print(f"Đã chèn {len(data)} sản phẩm vào bảng products_tiki.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Lỗi khi chèn dữ liệu: {error}")

DATA_DIR = "/data"
json_files = [f for f in os.listdir(DATA_DIR) if f.startswith("products_") and f.endswith(".json")]

for file_name in json_files:
    file_path = os.path.join(DATA_DIR, file_name)
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            products = json.load(file)
            data_to_insert = [(p["id"], p["name"], p["url_key"], p["price"], p["description"], p["image_url"]) for p in products]
            if data_to_insert:
                insert_products(data_to_insert)
    except Exception as e:
        print(f"Lỗi khi xử lý {file_name}: {e}")

print("Hoàn thành chèn tất cả sản phẩm vào products_tiki!")
