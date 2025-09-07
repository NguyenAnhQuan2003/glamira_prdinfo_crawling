from urllib.parse import urlparse
import pandas as pd
from config.connect import get_mongo_client, get_collection, get_collection_name, MongoConfig
from config.dir.address import uri, db_name, collection, domains
import os
os.makedirs("output", exist_ok=True)

cfg = MongoConfig(
    uri=uri,
    db_name=db_name
)

client = get_mongo_client(cfg)
collection = get_collection_name(client, cfg.db_name, collection)

target_collections = [
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
]

batch_size = 100000
skip = 0
unique_domains = set()

pd.DataFrame(columns=["domain"]).to_csv(domains, index=False)

total_fetched = 0

while True:
    cursor = (
        collection.find(
            {"collection": {"$in": target_collections}},
            {"current_url": 1, "_id": 0},
        )
        .skip(skip)
        .limit(batch_size)
    )

    urls = [doc["current_url"] for doc in cursor if "current_url" in doc]
    batch_count = len(urls)
    if batch_count == 0:
        break
    print(f"Fetched {batch_count} URLs in batch starting at record {skip}")
    total_fetched += batch_count
    batch_domains = []
    for url in urls:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        if domain:
            batch_domains.append(domain)
    batch_unique = set(batch_domains) - unique_domains
    unique_domains.update(batch_unique)

    df = pd.DataFrame(list(batch_unique), columns=["domain"])
    df.to_csv(domains, mode="a", header=False, index=False)
    skip += batch_size

print(f"Total URLs fetched: {total_fetched}")
print(f"Total unique domains saved: {len(unique_domains)}")