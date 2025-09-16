import logging

from logs.config_logs import setup_logging
from config.connect import MongoConfig, get_mongo_client, get_collection_name
from config.dir.address import uri, db_name, collection_products, collection_prdinfo
setup_logging()

try:
    cfg = MongoConfig(
        uri=uri,
        db_name=db_name,
    )
    client = get_mongo_client(cfg)
    collection_old = get_collection_name(client, cfg.db_name, collection_prdinfo)
    collection = get_collection_name(client, cfg.db_name, collection_products)
    logging.info("Connected to mongodb successfully!")
    print(collection_old, collection)
    pipeline = [
        {
            "$match": {
                "title": {"$regex": "^[A-Za-z0-9 ,.!?\"'-]+$"}
            }
        },
        {
            "$group": {
                "_id": "$product_id",
                "doc": {"$first": "$$ROOT"}
            }
        },
        {
            "$replaceRoot": {"newRoot": "$doc"}
        }
    ]
    result = list(collection_old.aggregate(pipeline))
    if result:
        collection.insert_many(result)
        print(f"Đã insert {len(result)} sản phẩm vào 'products'")
    else:
        logging.error("Insert failed!")
except Exception as e:
    logging.error("Error connect DB products: {}".format(e))