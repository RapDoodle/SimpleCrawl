from pymongo import MongoClient, InsertOne, UpdateOne
from abc import ABC, abstractclassmethod

class Store(ABC):
    @abstractclassmethod
    def add(self, data):
        pass

    @abstractclassmethod
    def update(self, data):
        pass

    @abstractclassmethod
    def commit(self):
        pass

class MongoDBStore(Store):
    def __init__(self, mongodb_url):
        self.mongodb_url = mongodb_url
        self.client = MongoClient(self.mongodb_url)
        self.database = self.client['scrawl']
        self.collection = self.database['webpages']
        self.write_buffer = []

    def add(self, data):
        self.write_buffer.append(InsertOne(data))

    def update(self, filter, update, upsert=False):
        self.write_buffer.append(UpdateOne(filter=filter, update=update, upsert=upsert))

    def commit(self):
        if len(self.write_buffer) > 0:
            self.collection.bulk_write(self.write_buffer)
            self.write_buffer.clear()

class LocalStore(Store):
    def add(self, data):
        pass

    def commit(self):
        pass