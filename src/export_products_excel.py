import logging

import pandas as pd
from config.connect import MongoConfig, get_collection_name, get_mongo_client
from config.dir.address import uri, collection_products, db_name, output_dir_export
from logs.config_logs import setup_logging

setup_logging()

try:
    cfg = MongoConfig(
        uri=uri,
        db_name=db_name,
    )
    client = get_mongo_client(cfg)
    collection = get_collection_name(client, cfg.db_name, collection_products)
    data = list(collection.find())
    df = pd.DataFrame(data)
    df.to_csv(output_dir_export, index=False)
except Exception as e:
    logging.error("Error connecting to mongodb: {}".format(e))