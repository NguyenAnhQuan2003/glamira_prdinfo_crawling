import json
import logging
import os
from config.dir.address import uri, db_name, collection_prdinfo, output_dir
from tqdm import tqdm
from config.connect import get_mongo_client, MongoConfig, get_collection_name
from logs.config_logs import setup_logging

setup_logging()

cfg = MongoConfig(
    uri=uri,
    db_name=db_name,
)
try:
    client = get_mongo_client(cfg)
    collection = get_collection_name(client, cfg.db_name, collection_prdinfo)
    logging.info("Connected to mongodb")

    JSON_DIR = output_dir
    for filename in tqdm(os.listdir(JSON_DIR)):
        if filename.endswith(".json"):
            filepath = os.path.join(JSON_DIR, filename)
            logging.info(f"Processing {filename}")
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        collection.insert_many(data)
                    else:
                        collection.insert_one(data)
            except Exception as e:
                logging.error("Filename error: {}".format(e))
except Exception as e:
    logging.error("Error connecting to mongodb! {}".format(e))


