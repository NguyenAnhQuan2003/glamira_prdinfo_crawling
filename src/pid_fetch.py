import time

import pandas as pd
from config.connect import get_mongo_client, get_collection, get_collection_name, MongoConfig
from config.dir.address import uri, db_name, collection, domains

cfg = MongoConfig(
    uri=uri,
    db_name=db_name
)

client = get_mongo_client(cfg)
collection = get_collection_name(client, cfg.db_name, collection)
print("✅ Kết nối thành công!")

batch_size = 100000

target_collections = [
    "view_product_detail", "select_product_option", "select_product_option_quality",
    "add_to_cart_action", "product_detail_recommendation_visible", "product_detail_recommendation_noticed",
    "product_view_all_recommend_clicked"
]

query = {
    "collection": {"$in": target_collections}
}

projection = {
    "collection": 1,
    "product_id": 1,
    "viewing_product_id": 1,
    "_id": 0
}
print("🔍 Đang truy vấn dữ liệu từ collection 'summary'...")
cursor = collection.find(query, projection)
product_ids = set()
for doc in cursor:
    pid = doc.get("product_id") or doc.get("viewing_product_id")
    if pid:
        product_ids.add(pid)

df = pd.DataFrame(list(product_ids), columns=["product_id"])

chunk_size = 3500
for i, start in enumerate(range(0, len(df), chunk_size), start=1):
    chunk = df.iloc[start:start + chunk_size]
    chunk.to_csv(f"pid{i}.csv", index=False)
    time.sleep(0.5)

print(f"Đã lưu {((len(df) - 1) // chunk_size) + 1} file CSV chứa {len(df)} product_id duy nhất.")