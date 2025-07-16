import os
import logging
from pymongo import MongoClient
from bson.json_util import dumps as bson_dumps
from google.cloud import storage

MONGO_URI = "mongodb://toan:minhtoan2004@localhost:27017"
DB_NAME = "countlydb"
COLLECTIONS = ["summary", "location"]

BATCH_SIZE = 100000
GCS_BUCKET_NAME = "project5de"
GCP_CREDENTIALS_PATH = "gcs_key.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS_PATH
gcs_client = storage.Client()
bucket = gcs_client.bucket(GCS_BUCKET_NAME)

def export_collection_to_jsonl_and_gcs(collection_name):
    collection = db[collection_name]
    total_docs = collection.estimated_document_count()
    logging.info(f"Exporting '{collection_name}' collection: ~{total_docs} documents...")

    local_filename = f"{collection_name}.jsonl"

    with open(local_filename, "w", encoding="utf-8") as f:
        cursor = collection.find({}, no_cursor_timeout=True)
        count = 0

        for doc in cursor:
            try:
                line = bson_dumps(doc)
                if line.strip():
                    f.write(line + "\n")
                count += 1
                if count % BATCH_SIZE == 0:
                    logging.info(f"Written {count} documents from '{collection_name}'")
            except Exception as e:
                logging.error(f"Error dumping document at count={count}: {e}")
        
        cursor.close()
        logging.info(f"Finished writing {count} documents from '{collection_name}' to '{local_filename}'")

    blob = bucket.blob(local_filename)
    blob.upload_from_filename(local_filename)
    logging.info(f"Uploaded '{local_filename}' to GCS bucket '{GCS_BUCKET_NAME}'")

if __name__ == "__main__":
    for col in COLLECTIONS:
        export_collection_to_jsonl_and_gcs(col)