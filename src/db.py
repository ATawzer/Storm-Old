from pymongo import MongoClient
import os
import json
from dotenv import load_dotenv
load_dotenv()

def get_storm_client():
    """
    Based on config_secret.json will connect into the storm MongoDB database
    """

    # Connect to DB using .env
    client = MongoClient(os.getenv('mongodb_uri'),
                        tls=True,
                        tlsCertificateKeyFile=os.getenv('ssl_path'))
    
    return client