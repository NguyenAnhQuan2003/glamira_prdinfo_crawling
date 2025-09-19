import json
import logging
from io import StringIO

from logs.config_logs import setup_logging
from config.connect import MongoConfig, get_mongo_client, get_collection_name
from config.dir.address import uri, db_name, collection_products, collection, collection_location
from google.cloud import storage

setup_logging()

def export_to_gcs(
        mongo_config: MongoConfig,
        collection_name: str,
        gcs_bucket_name: str,
):
    try:
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(gcs_bucket_name)
        logging.info("Connecting to Bucket!")

        client = get_mongo_client(mongo_config)
        collection = get_collection_name(client, mongo_config.db_name, collection_name)

        cursor = collection.find()
        record_count = 0

        jsonl_buffer = StringIO()

        for document in cursor:
            if '_id' in document:
                document['_id'] = str(document['_id'])
            jsonl_buffer.write(json.dumps(document) + '\n')
            record_count += 1
        file_name = f"{collection_name}.jsonl"
        blob = bucket.blob(file_name)
        blob.upload_from_string(jsonl_buffer.getvalue(), content_type="application/json")
        client.close()
    except Exception as e:
        logging.error("Error exporting GCS! {}".format(e))

def export_multiple_collections(
        mongo_config: MongoConfig,
        collection_name: list,
        gcs_bucket_name: str,
):
    try:
        for collection in collection_name:
            logging.info(f"Bắt đầu xuất collection: {collection}")
            export_to_gcs(mongo_config, collection, gcs_bucket_name)
            logging.info(f"Hoàn thành xuất collection: {collection}")
    except Exception as e:
        logging.error("Error exporting GCS {}".format(e))

if __name__ == "__main__":
    try:
        mongo_config = MongoConfig(
            uri=uri,
            db_name=db_name
        )
        COLLECTIONS = [collection, collection_products, collection_location]
        GCS_BUCKET = "data_glamira_behavier"
        export_multiple_collections(mongo_config = mongo_config, collection_name=COLLECTIONS, gcs_bucket_name=GCS_BUCKET)
    except Exception as e:
        logging.error("Error exporting GCS! {}".format(e))
