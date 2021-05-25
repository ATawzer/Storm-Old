# Modeling
import lightgbm
import shortuuid
from sklearn.metrics import accuracy_score, precision_score, recall_score
import numpy as np
import pandas as pd
from tqdm import tqdm
import os
import datetime as dt
import time
import json
from lightgbm import LGBMClassifier, Dataset, sklearn
import joblib
from dotenv import load_dotenv
load_dotenv() 

# Internal
from .db import *

class WeatherBoyPipeline:
    """
    Sources the feautures and targets for a set of tracks.
    Needs a pipline configuration
    """
    def __init__(self, tracks, storm_name, features={}, verbocity=2, train=False):

        self.X = pd.DataFrame()
        self.y = [] if train else None
        self.storm_name = storm_name
        self.tracks = tracks
        self.train = train
        self.features = features # MongoDB style

        # Database connection
        self.sdb = StormDB()

        # Verbocity
        self.print = print if verbocity > 0 else lambda x: None
        self.tqdm = lambda x: tqdm(x, leave=False) if verbocity > 1 else lambda x: x

    def load(self):
        """
        Primary Pipeline orchestration.
        """
        self.print(f"Loading Features for {len(self.tracks)} tracks.")
        self.load_X()

        if self.train:
            self.print("Loading Targets.")
            self.load_y()

    def as_lgbm(self, targets=True, **kwargs):
        """
        Converts the pipeline into training ready data for the LGBM CLassifier.
        This is ready to feed directly in
        """
        if targets:
            return Dataset(self.X, label=self.y, **kwargs)
        else:
            return Dataset(self.X)

    def load_X(self):
        """
        Get Feature Set, currently just the basic ones pulled out of Spotify.
        In a future state, there will be 3 main sets of features:
        - tracks (included now, spotify's features for now)
        - albums (feature engineering at album level)
        - artists (feature engineering at artist level)
        """

        self.X = pd.DataFrame(self.sdb.get_track_info(self.tracks, self.features))

    def load_y(self):
        """
        Looks for track membership in the good targets for the given tracks.
        """
        target_playlist_id = self.sdb.get_config(self.storm_name)['good_targets']
        target_tracks = self.sdb.get_loaded_playlist_tracks(target_playlist_id)
        self.y = [1 if track in target_tracks else 0 for track in self.tracks]

    def get_info(self):
        """
        Returns the parameters and configuration decisions made for pipeline
        """
        return {
            'tracks':self.tracks,
            'storm_name':self.storm_name,
            'features':self.X.columns
        }


class WeatherBoyModel:
    """
    Trains weatherboy(s) given a pipeline configuration.
    """
    def __init__(self, model_name=None, model_type='lgbm', hyper_params={}, output_dir='/models', verbocity=2, train=False):

        self.is_fit = False
        self.output_dir = output_dir
        if model_name is None:
            self.model_name = f'{self.model_type}_{dt.datetime.now().strftime("%y%m%d")}_{shortuuid.ShortUUID().random(length=6)}'
        else:
            self.model_name = model_name

        self.model_type = model_type
        self.hyper_params = hyper_params

        # Verbocity
        self.print = print if verbocity > 0 else lambda x: None
        self.tqdm = lambda x: tqdm(x, leave=False) if verbocity > 1 else lambda x: x

    # Housekeeping
    def load(self):
        """
        Will load in the model information or create a new instance
        if it isn't found
        """

        if self.model_type == 'lgbm':
            if os.path.exists(f"{self.output_dir}/{self.model_name}/model.txt"):
                self.model = LGBMClassifier().fit(init_model=f"{self.output_dir}/{self.model_name}/model.txt")
                self.is_fit = True
            else:
                raise FileNotFoundError(f"{self.model_name} not found.")

    # Core Methods
    def fit(self, wbp):
        """
        Fits a model given the configuration.
        Only gets called if in Train mode
        Pass in a loaded weatherboy pipeline to use.
        """

        if self.model_type == 'lgbm':
            self.model = LGBMClassifier(**self.hyper_params)
            self.model.fit(wbp.as_lgbm())
            
        # Save Model
        self.register(self.wbp.get_info())

    def save_model(self):
        """
        Saves the model given its type
        """

        if self.model_type == 'lgbm':
            self.model.save_model(f"{self.output_dir}/{self.model_name}/model.txt")


    def evaluate(self, wbp):
        """
        Runs an evaluation of a model (suite of metrics)
        Only gets called if in Train mode
        """
        pred = self.predict(wbp.X)
        eval_metrics = {}

        eval_metrics['acc'] = accuracy_score(y, pred)
        eval_metrics['acc'] = precision_score(y, pred)
        eval_metrics['acc'] = recall_score(y, pred)

        return eval_metrics

    def predict(self, wbp):
        """
        Generate scores for a feature set
        """

        if self.model_type == 'lgbm':
            return self.model.predict(wbp.as_lgbm(targets=False))

    def register(self, pipeline_info):
        """
        Saves a trained model and pipeline
        """

        # Save Model
        self.save_model()

        config = {
            'model_type':self.model_type,
            'parameters':self.hyper_params,
            'model_name':model_name,
        }
        config.update(pipeline_info)

        # Save metadata
        with f"{self.output_dir}/{model_name}/model_meta.json" as f:
            json.dump(config, f)
        
