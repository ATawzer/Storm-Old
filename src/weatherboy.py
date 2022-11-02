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

class WeatherBoy:
    """
    Main Model Interface, handles loading models and returning
    predictions from them.
    """

    def __init__(self, sdb: StormDB, model_name, model_dir: str='../models/'):

        self._sdb = sdb

        self.model_name = model_name
        self._model_dir = model_dir
        self._load_model()

    def generate_prediction_scores(self, track_ids):
        """
        Runs Data pipeline and scores based on the configured model.
        """

        l.debug(f"Sourcing Data for prediction on {len(track_ids)} tracks.")
        track_df = pd.DataFrame.from_records(self._sdb.get_track_info(track_ids, fields={"last_updated":0}))
        return self._model.predict_proba(track_df)

    def _load_model(self):

        try:
            self._model = joblib.load(f"{self._model_dir}{self.model_name}/.pkl")
        except:
            ValueError(f"Could not load {self._model_dir}, does the model exist? Was it serialized properly?")

    def rank_tracks(self, tracks, good_playlist, great_playlist):
        """
        Ranks storm tracks. 
        """

        df = pd.DataFrame(
            {
                'track_id':tracks,
                'prediction_scores':self.generate_prediction_scores(tracks)
            }
        )

        return df.sort_values('prediction_scores', descending=True)['track_id'].tolist()