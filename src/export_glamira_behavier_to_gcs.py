import logging

from config.dir.address import uri, db_name, collection
from logs.config_logs import setup_logging
from config.connect import MongoConfig, get_mongo_client, get_collection_name
from datetime import datetime
import os
import json
from google.api_core import retry
from google.cloud import storage

setup_logging()

OUTPUT_DIR = "./data/exports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

MERGED_DIR = "./data"
os.makedirs(MERGED_DIR, exist_ok=True)


def clean_document(document: dict) -> dict:
    def recursive_clean(data, parent_key=None):
        if isinstance(data, dict):
            return {k: recursive_clean(v, k) for k, v in data.items()}
        elif isinstance(data, list):
            return [recursive_clean(item, parent_key) for item in data]
        elif isinstance(data, datetime):
            return data.isoformat()
        elif data == "" and parent_key in ["option", "cart_products"]:
            return []
        return data
    return recursive_clean(document)

def export_sumary(
        mongo_config: MongoConfig,
        collection_name: str,
        output_dir: str,
        batch_size: int = 10000,
        records_per_file: int = 1000000
):
    try:
        os.makedirs(output_dir, exist_ok=True)
        client = get_mongo_client(mongo_config)
        collection = get_collection_name(client, mongo_config.db_name, collection_name)

        # Đếm tổng số bản ghi
        total_records = collection.count_documents({})
        logging.info(f"Total records in {collection_name}: {total_records}")

        # Xuất dữ liệu theo lô
        skip = 0
        file_index = 0
        record_count = 0

        while skip < total_records:
            cursor = collection.find().skip(skip).limit(records_per_file).batch_size(batch_size)
            out_path = os.path.join(output_dir, f"{collection_name}_part{file_index}.jsonl")
            outfile = open(out_path, "w", encoding="utf-8")

            batch_data = []
            for document in cursor:
                if '_id' in document:
                    document['_id'] = str(document['_id'])
                cleaned_doc = clean_document(document)
                batch_data.append(cleaned_doc)
                record_count += 1

                if len(batch_data) >= batch_size:
                    for doc in batch_data:
                        outfile.write(json.dumps(doc, ensure_ascii=False) + "\n")
                    batch_data = []
                    logging.info(f"Processed {batch_size} records for {out_path}")

            if batch_data:  # Ghi các bản ghi còn lại
                for doc in batch_data:
                    outfile.write(json.dumps(doc, ensure_ascii=False) + "\n")
                logging.info(f"Processed remaining {len(batch_data)} records for {out_path}")

            outfile.close()
            logging.info(f"Completed {out_path} with {record_count} records so far")
            skip += records_per_file
            file_index += 1

        client.close()
        logging.info(f"Total records exported: {record_count}")
    except Exception as e:
        logging.error(f"Error exporting {collection_name}: {str(e)}")
        raise
    finally:
        if 'outfile' in locals():
            outfile.close()
        client.close()

def merge_jsonl_files(input_dir: str, merged_file: str, prefix: str):
    try:
        logging.info(f"Merging files for {prefix} into {merged_file}")
        with open(merged_file, "w", encoding="utf-8") as outfile:
            for filename in sorted(os.listdir(input_dir)):
                if filename.startswith(prefix) and filename.endswith(".jsonl"):
                    file_path = os.path.join(input_dir, filename)
                    logging.info(f"Processing {file_path}")
                    with open(file_path, "r", encoding="utf-8") as infile:
                        for line in infile:
                            outfile.write(line)
                    os.remove(file_path)  # Xóa file phần
        logging.info(f"Successfully merged into {merged_file}")
    except Exception as e:
        logging.error(f"Error merging {prefix}: {str(e)}")
        raise

@retry.Retry(predicate=retry.if_exception_type(Exception), initial=1.0, maximum=60.0, multiplier=2.0, timeout=300.0)
def upload_to_gcs(bucket_name: str, source_file: str, destination_blob: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    logging.info(f"Uploaded {source_file} -> gs://{bucket_name}/{destination_blob}")

def main():
    try:
        mongo_config = MongoConfig(
            uri=uri,
            db_name=db_name
        )
        collection_name = collection
        gcs_bucket_name = "data_glamira_behavier"
        merged_file = os.path.join(MERGED_DIR, f"{collection_name}.jsonl")
        logging.info(f"Starting export for {collection_name}")
        export_sumary(mongo_config, collection_name, OUTPUT_DIR)
        merge_jsonl_files(OUTPUT_DIR, merged_file, collection_name)
        upload_to_gcs(gcs_bucket_name, merged_file, f"{collection_name}.jsonl")
        os.remove(merged_file)
        logging.info(f"Cleaned up local file {merged_file}")
    except Exception as e:
        logging.error("Error export {} ".format(e))

if __name__ == "__main__":
    main()