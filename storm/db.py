# Handles writing to MongoDB
from pymongo import MongoClient

class StormDB:

    def __init__(self):
        self.client = MongoClient(
            host=os.environ.get('MONGO_HOST', 'localhost:27017'),
            

        )
        self.db = self.client.storm