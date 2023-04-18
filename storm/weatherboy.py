# Modeling
import numpy as np
import pandas as pd
from tqdm import tqdm
import joblib

from dotenv import load_dotenv
load_dotenv() 

# Directory writing
import os

# Internal
from .db import *
from .modeling import StormTrackClusterizer, MeanSquasher, FeatureSelector
from .storm_client import StormUserClient

class WeatherBoy:
    """
    Main Model Interface, handles loading models and returning
    predictions from them.
    """

    def __init__(self, sdb: StormDB, model_name, model_dir: str='../models/', friendly_name='{cluster_number}'):

        self.sdb = sdb
        self.model_name = model_name
        self.model_dir = model_dir
        self.friendly_name = friendly_name

    def run(self, tracks: List[str]):
        """
        Runs tracks through the model
        """

        model = StormTrackClusterizer(dir='./models', storm_db_client=self.sdb)
        model.load_model_by_name(self.model_name)

        predicted = model.predict(tracks)
        results = model.format_track_predictions_for_writing(predicted, self.friendly_name)

        storm_client = StormUserClient(os.getenv('spotify_user_id'))

        playlist_info = []
        for i, name in enumerate(list(results.keys())):
            playlist_info.append({
                'name':name
            })

        for playlist_name, tracks in results.items():
            storm_client.write_playlist_tracks_by_name(playlist_name, tracks)