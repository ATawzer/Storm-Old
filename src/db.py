from pymongo import MongoClient
import os
import json

def get_config_secret(path):
    """
    Get the secret configuration for the client running the storm.

    :param path: str path, where config_secret will be
    :returns: config_secret json object
    """
    if os.path.exists(path+"/config_secret.json"):
        cs = json.load(open(path+"/config_secret.json", "r"))

        # check if has good info
        if "path_to_secret" not in cs.keys():
            raise ValueError("path_to_secret not found in config_secret. Must specify for authentication.")

        return cs
    else:
        raise FileNotFoundError("config_secret.json not found, must create with a path_to_secret key for authentication.")

def get_config_public(path):
    """
    Get the secret configuration for the client running the storm.

    :param path: str path, where config_secret will be
    :returns: config_public json object
    """
    if os.path.exists(path+"/config_public.json"):
        return json.load(open(path+"/config_public.json", "r"))
    else:
        raise FileNotFoundError("config_secret.json not found, must create with a path_to_secret key for authentication.")

def get_storm_client(config_dir):
    """
    Based on config_secret.json will connect into the storm MongoDB database
    """

    # Load in secret/public configuration
    cs = get_config_secret(config_dir)
    cp = get_config_public(config_dir)

    # Connect to DB
    client = MongoClient(cp['mongodb_uri'],
                        tls=True,
                        tlsCertificateKeyFile=cs['path_to_secret'])
    
    return client