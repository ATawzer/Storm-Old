import logging
from typing import Dict
import pandas as pd
import os

from typing import List, Dict, Any
from sklearn.pipeline import Pipeline
from sklearn.base import TransformerMixin, BaseEstimator

from uuid import uuid4
import joblib

from .db import StormDB

l = logging.getLogger('storm.modeling')

MODEL_NAME_BASE_FORMAT = '{friendly_name}__{storm_model_class}__{num_clusters}__{run}'

class StormTrackClusterizer:
    """
    Manages loading and scoring of 'storm tracks', or more
    generally a set of track ids. Feature level is
    at the track id level.
    """

    def __init__(self, dir: str='../models', storm_db_client: StormDB=None):
        self.dir = dir

        self.storm_db = storm_db_client
        self._model = None

    def load_model_by_name(self, name: str):
        """
        Loads a model from prod or dev given exact name
        """

        if name+'.pkl' in os.listdir(self.dir):
            self._model = joblib.load(f"{self.dir}/{name}.pkl")
        else:
            raise FileNotFoundError(f"Can't find {name}.pkl")

    def predict(self, track_ids: List[str]) -> pd.DataFrame:
        """
        Returns predicted class and distance to cluster
        """

        if self._model is None:
            raise Exception("Model not loaded, call StormTrackClusterizer.load_model_by_name first")

        track_df = pd.DataFrame.from_records(self.storm_db.get_track_info(track_ids))
        track_df['cluster'] = self._model.predict(track_df)
        track_df['distance_to_cluster'] = self._model.transform(track_df).min(axis=1)

        return track_df[['_id', 'cluster', 'distance_to_cluster']]

    @staticmethod
    def register_model(model_name: str, fitted_pipeline: Pipeline, num_clusters: int, directory='../models'):
        """
        Saves a model to the directory with consistent formatting
        """

        output_name = MODEL_NAME_BASE_FORMAT.format(friendly_name=model_name, storm_model_class='track_feature', num_clusters=num_clusters, run=uuid4())
        joblib.dump(fitted_pipeline, f'../models/{output_name}.pkl', compress = 1)

        l.info(f"{output_name} saved to {directory}")
        return output_name

    @staticmethod
    def format_track_predictions_for_writing(predictions: pd.DataFrame, playlist_name_format:str) -> Dict:
        """
        Formats the ouputs of the prediction fo writing, with playlist_names
        Input, pandas dataframe with track_id, cluster, distance to cluster
        Output, "playlist_name":"Track Ids, ordered by distance"
        """

        output = {playlist_name_format.format(cluster_number=x):[] for x in predictions.cluster.unique()}

        for cluster_number in predictions.cluster.unique():
            output[playlist_name_format.format(cluster_number=cluster_number)] = predictions[predictions.cluster == cluster_number].sort_values('distance_to_cluster', ascending=True)._id

        return output

# ===============
# SKLearn Helpers
# ===============
class FeatureSelector(BaseEstimator, TransformerMixin):
    """
    SKLearn Pipeline step allowing for filtering of Pandas Dataframe.
    ===========
    Parameters:
        feature_names - List - the names of the columns in the Pandas Dataframe to filter
    """
    
    def __init__(self, feature_names: List=[]):
        self.feature_names = feature_names

    def fit(self, X:pd.DataFrame, y = None):
        return self 

    def transform(self, X:pd.DataFrame, y = None) -> pd.DataFrame:
        return X[self.feature_names]

class MeanSquasher(BaseEstimator, TransformerMixin):
    """
    Squashes features close to their mean to 0.
    For this to have intended effect data must be normalized.

    ===========
    Parameters:
        feature_names - List - the names of the columns in the Pandas Dataframe to filter
    """
    
    def __init__(self, threshold=1):
        self.threshold = threshold

    def fit(self, X:pd.DataFrame, y = None):
        return self 

    def transform(self, X:pd.DataFrame, y = None) -> pd.DataFrame:
        X[(X < self.threshold)&(X > -self.threshold)] = 0
        return X