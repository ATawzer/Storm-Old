import spotipy
from spotipy import util
from spotipy import oauth2
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
from src.db import *
from src.storm import Storm

config = {
        'pipeline_cfg':{'supervision_table':'inferred_supervised_storm_tracks',
            'storm_name':'film_vg_instrumental',
            'start_date':'2018-01-01',
            'end_date':'2021-05-05',
            'base_data':{
                'columns':{'logic':'exclude', 'names':['run_id', 'name', 'run_date', 'album_id', 'audio_features', 'last_updated']}
            },
            'pre_split_transformations':['fill_missing'],
            'partioning':{
                'freq':'w'
            },
            #'post_split_transformations':['']
            'train_partioning':{
                'X_cols':{'logic':'exclude', 'names':['target_track']},
                'y_col':'target_track',
                'train_test_split':False
            }
        }
}


# Initialize a base pipeline (from which a data dictionary can be generated)
pipeline = WeatherBoyPipeline(config['pipeline_cfg'], mode='inferred')
pipeline.Load()

