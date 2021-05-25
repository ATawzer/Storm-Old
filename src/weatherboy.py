# Modeling
import numpy as np
import pandas as pd
from tqdm import tqdm
import os
import datetime as dt
import time
import json
import joblib
from dotenv import load_dotenv
load_dotenv() 

# Internal
from .db import *

class WeatherBoy:
    """
    Endpoint prediction, leverages pre-trained models.
    Loads a model using a configuration_id.
    If it doesn't know one it will default to that storm's best guess.

    WeatherMan can control models and pipelines after instantiation separately
    for rapid experiementation but on production runs it will run
    the entirety of the process.
    """

    def __init__(self, tracks, storm_name, weather_boy_config_id=None, verbocity=3):

        self.storm = storm_name
        self.cfg_id = weather_boy_config_id
        self.tracks = tracks
        self.scores = {track:0 for track in tracks}

        # To be loaded
        self.model_cfg = {}
        self.pipeline_cfg = {}

        # Load
        self.load_config(weather_boy_config_id)

        # pipeline
        self.pipeline = WeatherBoyPipeline(tracks, self.pipeline_cfg, verbocity=verbocity-1)
        self.pipeline.load()

        # model
        self.model = WeatherBoyModel(self.model_cfg, verbocity=verbocity-1)
        self.model.load()

    def load_config(self):
        """
        Loads the model and pipeline configurations from the SDB.
        """

        cfg = self.sdb.get_wb_config(self.cfg_id)
        self.pipeline_cfg, self.model_cfg = cfg['pipeline'], cfg['model']

    def get_scores(self):
        """
        Given a loaded model and pipeline, get the prediction scores per track.
        """
    
        self.scores = self.model.Predict(self.pipeline['X']) # returns in dict format


    def rank_order():

        return False

class WeatherBoyPipeline:
    """
    Sources the feautures and targets for a set of tracks.
    Needs a pipline configuration
    """
    def __init__(self, tracks, config, verbocity=2, train=False):

        self.X = pd.DataFrame()
        self.y = [] if train else None
        self.tracks = tracks
        self.cfg = config
        self.train = train

        # Database connection
        self.sdb = StormDB()

        # Verbocity
        self.print = print if verbocity > 0 else lambda x: None
        self.tqdm = lambda x: tqdm(x, leave=False) if verbocity > 1 else lambda x: x

        self.validate_configuration()

    def validate_configuration(self):
        """
        Checks for a valid configuration.
        """
        self.print("Checking for configuration validity.")

        check_keys = ['storm_name']

        if not set(check_keys).issubset(set(self.cfg.keys())):
            raise KeyError(f"Configuration missing parameters. Specify: {set(check_keys) - set(self.cfg.keys())}")

        self.print("Configuration is valid!")

    def load(self):
        """
        Primary Pipeline orchestration.
        """
        self.print(f"Loading Features for {len(self.tracks)} tracks.")
        self.load_X()

        if self.train:
            self.print("Loading Targets.")
            self.load_y()

        self.print("Pipeline Loaded!\n")

    def load_X(self):
        """
        Get Feature Set, currently just the basic ones pulled out of Spotify.
        In a future state, there will be 3 main sets of features:
        - tracks (included now, spotify's features for now)
        - albums (feature engineering at album level)
        - artists (feature engineering at artist level)
        """

        self.X = pd.DataFrame(self.sdb.get_track_info(self.tracks))

    def load_y(self):
        """
        Looks for track membership in the good targets for the given tracks.
        """
        target_playlist_id = self.sdb.get_config(self.cfg['storm_name'])['good_targets']
        target_tracks = self.sdb.get_loaded_playlist_tracks(target_playlist_id)
        self.y = [1 if track in target_tracks else 0 for track in self.tracks]


class WeatherBoyModel:
    """
    Trains weatherboy(s) given a pipeline configuration.
    """
    def __init__(self, config, verbocity=2, train=False):

        self.cfg = config
        self.is_fit = False

        # Verbocity
        self.print = print if verbocity > 0 else lambda x: None
        self.tqdm = lambda x: tqdm(x, leave=False) if verbocity > 1 else lambda x: x

    def validate_configuration(self):
        """
        Checks for a valid configuration.
        """
        self.print("Checking for configuration validity.")

        check_keys = ['model_path']

        if not set(check_keys).issubset(set(self.cfg.keys())):
            raise KeyError(f"Configuration missing parameters. Specify: {set(check_keys) - set(self.cfg.keys())}")

        self.print("Configuration is valid!")

    # Housekeeping
    def load(self):
        """
        Will load in the model information or create a new instance
        if it isn't found
        """

        if os.path.exists(f"{self.cfg['model_path']}/{self.cfg['model_id']}"):
            self.print(f"Model {self.cfg['model_id']} found! Loading in.")
            self.model = joblib.load(f"{self.cfg['model_path']}/{self.cfg['model_id']}")
        else:
            self.print(f"No Model found for {self.cfg['model_id']}")
            self.model = LightGBMClassifier()

        # Try to load in model info
        if 'model_id' in self.cfg.keys():
            self.model_id = self.cfg['model_id']
            self.load_model(self.model_id)

    # Core Methods
    def fit(self, X, y):
        """
        Fits a model given the configuration.
        Only gets called if in Train mode
        """

    def evaluate(self, X, y):
        """
        Runs an evaluation of a model (suite of metrics)
        Only gets called if in Train mode
        """

    def predict(self, X):
        """
        Generate scores for a feature set
        """

    


