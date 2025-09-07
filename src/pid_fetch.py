import pandas as pd
from config.connect import get_mongo_client, get_collection, get_collection_name, MongoConfig
from config.dir.address import uri, db_name, collection, domains

cfg = MongoConfig(
    uri=uri,
    db_name=db_name
)

client = get_mongo_client(cfg)
collection = get_collection_name(client, cfg.db_name, collection)

batch_size = 100000

cursor = collection.find({}, {"product_id": 1, "_id": 0})
product_ids = [doc["product_id"] for doc in cursor if "product_id" in doc]

df = pd.DataFrame(product_ids, columns=["product_id"]).drop_duplicates()

chunk_size = 3500
for i, start in enumerate(range(0, len(df), chunk_size), start=1):
    chunk = df.iloc[start:start + chunk_size]
    chunk.to_csv(f"node{i}.csv", index=False)

print(f"Unique product_ids saved to {((len(df) - 1) // chunk_size) + 1} files.")