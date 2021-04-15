import os
import json
from pymongo import MongoClient

from dotenv import load_dotenv
load_dotenv()

class StormDB:
    """
    Manages the Dynamodb connections, reading and writing.
    """
    def __init__(self):

        # Build mongo client and db
        self.mc = MongoClient(os.getenv('mongo_uri'))
        self.db = self.mc[os.getenv('storm_db')]

        # initialize collections
        self.artists = self.db['artists']
        self.albums = self.db['albums']
        self.storms = self.db['storm_metadata']
        self.tracks = self.db['tracks']

    