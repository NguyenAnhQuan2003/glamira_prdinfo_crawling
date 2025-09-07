from dataclasses import dataclass
from pymongo.collection import Collection
from pymongo import MongoClient

@dataclass
class MongoConfig:
    uri: str
    db_name: str

def get_mongo_client(cfg: MongoConfig) -> MongoClient:
    return MongoClient(cfg.uri)


def get_collection(db, name: str) -> Collection:
    return db[name]

def get_collection_name(client: MongoClient, db_name: str, name: str) -> Collection:
    return client[db_name][name]