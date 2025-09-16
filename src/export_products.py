import logging

import pandas as pd
from config.connect import MongoConfig, get_collection_name, get_mongo_client
from config.dir.address import uri, collection_products, db_name, output_dir_export, output_dir_jsonl
from logs.config_logs import setup_logging
import json
setup_logging()

try:
    cfg = MongoConfig(
        uri=uri,
        db_name=db_name,
    )
    client = get_mongo_client(cfg)
    collection = get_collection_name(client, cfg.db_name, collection_products)
    data = list(collection.find())
    cleaned_data = []
    for doc in data:
        doc_copy = dict(doc)
        doc_copy.pop("_id", None)
        cleaned_data.append(doc_copy)
    with open(output_dir_jsonl, "w", encoding="utf-8") as f:
        for doc in cleaned_data:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    logging.info("Export jsonl successfully!")
    df = pd.DataFrame(data)
    df.to_csv(output_dir_export, index=False)
    logging.info("Export csv successfully!")
except Exception as e:
    logging.error("Error connecting to mongodb: {}".format(e))