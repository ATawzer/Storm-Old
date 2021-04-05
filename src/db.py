from pymongo import MongoClient
import os

def get_config_secret():
    """
    Get the secret configuration for the client running the storm.


    """


    if os.path.exists()

def get_config_public():
    """
    Gets the public configuration file.
    """

def get_storm_client():
    """
    Based on config_secret.json will connect into the storm MongoDB database
    """

    # Load in secret/client configuration
    cs = get_config_secret()

    # Connect to DB
    client = MongoClient(uri,
                        tls=True,
                        tlsCertificateKeyFile=cs['path_to_secret'])
    
    return client