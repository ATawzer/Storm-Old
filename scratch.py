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
        'pipeline_cfg':{
            'storm_name':'film_vg_instrumental',
            'start_date':'2018-01-01',
            'end_date':'2021-05-05',
            'track_features':{"artists":0, "audio_analysis":0}
            #'pre_split_transformations':['']
            #'post_split_transformations':['']
        }
}


# Initialize a base pipeline (from which a data dictionary can be generated)
pipeline = WeatherBoyPipeline(config['pipeline_cfg'], mode='inferred')
pipeline.Load()

