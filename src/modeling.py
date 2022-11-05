import logging
from typing import Dict
import pandas as pd
import os

from typing import List, Dict, Any
from sklearn.pipeline import Pipeline
from sklearn.base import TransformerMixin, BaseEstimator

from uuid import uuid4
import joblib

l = logging.getLogger('storm.db')


class ModelManager:
    """
    Shared object for managing the preservation and movement of models
    for different storms within the same directory.
    """

    def __init__(self, dir: str='../models', base_format: str = '{storm_name}__{storm_model_class}__{run}'):
        self.dir = dir
        self.base_format = base_format

    def load_model_by_name(self, name: str, location ='/dev'):
        """
        Loads a model from prod or dev given exact name
        """

        if name+'.pkl' in os.listdir(self.dir+location):
            model = joblib.load(name+'.pkl')

            return model
        else:
            raise FileNotFoundError(f"Can't find {name}.pkl")

    def register_model(self, storm_name: str, storm_model_type: str, fitted_pipeline: Pipeline):
        """
        Saves a model to the directory with consistent formatting
        """

        output_name = self.base_format.format(storm_name, storm_model_type, uuid4())
        joblib.dump(fitted_pipeline, f'../models/dev/{output_name}.pkl', compress = 1)

        return output_name

    def promote_model(model_name, retire: bool=True):
        """
        Moves a model from dev to prod. This allows for preservation of dev and prod models.
        If a model of the type already exists in prod it will move it to archive.
        dev -> prod, if in prod then prod -> archive
        """

        None


    def restore_model(retire: bool=True):
        """
        Moves a model from dev to prod. This allows for preservation of dev and prod models.
        If a model of the type already exists in prod it will move it to archive.
        archive -> prod, if in prod then prod -> dev
        """

        None

    def get_prod_model(storm_name, storm_model_type):
        """
        Returns the loaded model from prod
        """
    
        None



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
    