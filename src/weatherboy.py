# Modeling
import numpy as np
import pandas as pd
from tqdm import tqdm
import os
import datetime as dt
import time
import json
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
        self.pipeline = WeatherBoyPipeline(tracks, pipeline_cfg, verbocity=verbocity-1)
        self.pipeline.Load()

        # model
        self.model = WeatherBoyModel(model_cfg, verbocity=verbocity-1)
        self.model.Load()

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
    def __init__(self, tracks, config, verbocity=2, mode='inferred', train=False):

        self.X = pd.DataFrame()
        self.y = []
        self.data = pd.DataFrame()
        self.cfg = config
        self.mode = mode
        self.train = train

        # Database connections
        self.sadb = StormAnalyticsDB()
        self.sdb = StormDB()

        # Verbocity
        self.print = print if verbocity > 0 else lambda x: None
        self.tqdm = lambda x: tqdm(x, leave=False) if verbocity > 1 else lambda x: x

        self.validate_configuration()

        # Pipeline reference
        self.pref = {'fill_missing':self.fill_missing}

    def validate_configuration(self):
        """
        Checks for a valid configuration.
        """
        self.print("Checking for configuration validity.")

        check_keys = ['storm_name', 'base_data', 'train_partioning', 'start_date', 'end_date']

        if not set(check_keys).issubset(set(self.cfg.keys())):
            raise KeyError(f"Configuration missing parameters. Specify: {set(check_keys) - set(self.cfg.keys())}")

        self.print("Configuration is valid!")

    def Load(self):
        """
        Primary Pipeline orchestration.
        """
        self.print("Initializing base data pipeline.")
        self.load_base()
        self.print("Done!\n") 

    def load_base(self):
        """
        Loads a base set of data by which to pair down for the specific configuration.
        In a future state, there will be 3 main sets of features:
        - tracks (included now, spotify's features for now)
        - albums (feature engineering at album level)
        - artists (feature engineering at artist level)
        """

        self.data = self.sdb.get_track_info(self.tracks, self.cfg['track_features']).set_index('_id')

        # Get targets if train
        if self.train:

            self.sdb.get_loaded_playlist_tracks()

class WeatherMan:
    """
    Orchestrates weatherboy training
    """

    def __init__(self):

        self.start = None

    # Data Dictionary Splits
    def partition_dd(self):
        """
        Paritions .data based on mode.
        """
        date_windows = []
        if self.mode == 'inferred':
            drange = pd.date_range(self.cfg['start_date'], self.cfg['end_date'], freq=self.cfg['partioning']['freq'])
            drange = [x.strftime('%Y-%m-%d') for x in drange]
            date_windows = [(drange[i], drange[i+1]) for i in range(len(drange)-1)]
        else:
            # Not suppored
            return False

        # Partition
        if self.mode == 'inferred':
            for window in self.tqdm(date_windows):
                self.dd[window[1]] = {}
                self.dd[window[1]]['dataset'] = self.data[(self.data['release_date'] < window[1]) & 
                                                          (self.data['release_date'] >= window[0])].copy()
        else:
            # Not supported
            return False

