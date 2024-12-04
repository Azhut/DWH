# client.py
from pymongo import MongoClient
from app.core.config import db

class MongoDBClient:
    def __init__(self):
        self.client = MongoClient(db.DB_URI)
        self.db = self.client[db.DB_NAME]

    def get_collection(self, collection_name: str):
        return self.db[collection_name]
