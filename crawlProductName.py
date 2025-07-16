import csv
import requests
import random
import time
from bs4 import BeautifulSoup
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

client = MongoClient('mongodb://localhost:27017/')
db = client['countlydb']
collection = db['summary']

query = {
    "collection": {"$in": ["view_product_detail", "select_product_option", "select_product_option_quality"]},
    "product_id": {"$exists": True, "$ne": ""}
}
cursor = collection.find(query, {"product_id": 1, "current_url": 1})

product_map = {}
for doc in cursor:
    pid = doc.get("product_id")
    url = doc.get("current_url")
    if pid and pid not in product_map:
        product_map[pid] = url

def fetch_product_name(product_id, url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(0.1, 0.5)) 
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                product_name_el = soup.select_one('h1.page-title > span.base') 
                if product_name_el:
                    return {
                        "product_id": product_id,
                        "product_name": product_name_el.text.strip(),
                        "current_url": url
                    }
                else:
                    return {
                        "product_id": product_id,
                        "product_name": "",
                        "current_url": url
                    }
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"❌ Lỗi {url}: {e}")
            time.sleep(1)  
    return None

results = []
max_threads = 30

with ThreadPoolExecutor(max_workers=max_threads) as executor:
    futures = []
    for product_id, url in product_map.items():
        futures.append(executor.submit(fetch_product_name, product_id, url))

    for future in tqdm(as_completed(futures), total=len(futures), desc="Đang crawl"):
        result = future.result()
        if result:
            results.append(result)

with open('product_names.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['product_id', 'product_name', 'current_url']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

print("✅ Đã hoàn tất lưu file product_names.csv")