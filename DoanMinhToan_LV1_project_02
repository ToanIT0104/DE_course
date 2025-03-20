import asyncio
import httpx
import pandas as pd
import json
import os
from tqdm.asyncio import tqdm

BATCH_SIZE = 1000
MAX_CONCURRENT_REQUESTS = 10
CSV_FILE = "products-0-200000.csv"
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
OUTPUT_FOLDER = "data"
ERROR_LOG = "error_log.txt"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def read_product_ids_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df.iloc[:, 0].dropna().astype(str).tolist()

async def fetch_product(client, product_id, semaphore, error_products):
    async with semaphore:
        try:
            url = API_URL.format(product_id)
            response = await client.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if "id" not in data:
                raise ValueError("Sản phẩm không tồn tại hoặc API lỗi")
            return {
                "id": data.get("id"),
                "name": data.get("name"),
                "url_key": data.get("url_key"),
                "price": data.get("price"),
                "description": data.get("description"),
                "image_url": data.get("thumbnail_url")
            }
        except Exception as e:
            error_message = f"Lỗi khi lấy dữ liệu sản phẩm {product_id}: {e}"
            print(f"❌ {error_message}")
            error_products.append({"product_id": product_id, "error": str(e)})
            return None

async def fetch_all_products(product_ids):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with httpx.AsyncClient() as client:
        tasks = []
        results = []
        error_products = []
        batch_count = 0
        for i, product_id in enumerate(product_ids):
            tasks.append(fetch_product(client, product_id, semaphore, error_products))
            if len(tasks) >= BATCH_SIZE or i == len(product_ids) - 1:
                batch_results = await tqdm.gather(*tasks, desc=f"📦 Đang lấy dữ liệu batch {batch_count}", ncols=100)
                results.extend([r for r in batch_results if r])
                with open(f"{OUTPUT_FOLDER}/products_{batch_count}.json", "w", encoding="utf-8") as f:
                    json.dump(results, f, ensure_ascii=False, indent=4)
                print(f"✅ Đã lưu {len(results)} sản phẩm vào {OUTPUT_FOLDER}/products_{batch_count}.json")
                with open(f"{OUTPUT_FOLDER}/errors_{batch_count}.json", "w", encoding="utf-8") as f:
                    json.dump(error_products, f, ensure_ascii=False, indent=4)
                print(f"❌ Đã lưu {len(error_products)} sản phẩm lỗi vào {OUTPUT_FOLDER}/errors_{batch_count}.json")
                batch_count += 1
                tasks = []
                results = []
                error_products = []

async def main():
    product_ids = read_product_ids_from_csv(CSV_FILE)
    print(f"📦 Tổng số sản phẩm cần lấy: {len(product_ids)}")
    await fetch_all_products(product_ids)

if __name__ == "__main__":
    asyncio.run(main())
