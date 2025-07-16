from pymongo import MongoClient, UpdateOne
import IP2Location
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed

BATCH_SIZE = 100000
NUM_PROCESSES = multiprocessing.cpu_count()
IP_TIMEOUT = 5 

MONGO_URI = "mongodb://toan:minhtoan2004@localhost:27017/?authSource=admin"

def init_worker():
    global ip2loc
    ip2loc = IP2Location.IP2Location("IP-COUNTRY-REGION-CITY.BIN")

def process_ip(ip):
    try:
        rec = ip2loc.get_all(ip)
        return {
            "ip": ip,
            "country": rec.country_long,
            "region": rec.region,
            "city": rec.city,
            "latitude": rec.latitude,
            "longitude": rec.longitude
        }
    except Exception:
        return None

def save_batch_to_db_bulk(results):
    client = MongoClient(MONGO_URI)
    db = client["countlydb"]
    location_collection = db["location"]
    operations = []
    for loc in results:
        if loc:
            operations.append(UpdateOne({"ip": loc["ip"]}, {"$set": loc}, upsert=True))
    if operations:
        location_collection.bulk_write(operations)

def get_unique_ips(db, collection_name, batch_size=BATCH_SIZE):
    pipeline = [{"$group": {"_id": "$ip"}}]
    cursor = db[collection_name].aggregate(pipeline, allowDiskUse=True)
    batch = []
    for doc in cursor:
        batch.append(doc["_id"])
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def process_batch_with_timeout(ip_batch):
    results = []
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES, initializer=init_worker) as executor:
        future_to_ip = {executor.submit(process_ip, ip): ip for ip in ip_batch}
        for future in as_completed(future_to_ip, timeout=IP_TIMEOUT * len(ip_batch)):
            ip = future_to_ip[future]
            try:
                result = future.result(timeout=IP_TIMEOUT)
                results.append(result)
            except Exception:
                print(f"⚠️ IP lỗi hoặc quá timeout: {ip}")
    return results

def process_all_ips():
    client = MongoClient(MONGO_URI)
    db = client["countlydb"]
    location_collection = db["location"]
    location_collection.create_index("ip", unique=True)

    main_collection_name = "summary"

    print("🔍 Lấy danh sách IP duy nhất bằng aggregate...")
    ip_batches = list(get_unique_ips(db, main_collection_name))
    total_batches = len(ip_batches)
    print(f"✅ Tổng số batch IP: {total_batches}")
    print(f"🚀 Số process sẽ chạy song song: {NUM_PROCESSES}")

    for i, ip_batch in enumerate(ip_batches):
        print(f"\n📦 Bắt đầu xử lý batch {i + 1}/{total_batches} (số IP: {len(ip_batch)})")
        results = process_batch_with_timeout(ip_batch)
        print(f"💾 Đang ghi batch {i + 1} vào MongoDB...")
        save_batch_to_db_bulk(results)
        print(f"✅ Batch {i + 1}/{total_batches} hoàn thành – {((i + 1) / total_batches) * 100:.2f}%")

    print("🎉 Hoàn tất xử lý toàn bộ IP!")

if __name__ == "__main__":
    process_all_ips()