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
from src.analytics import *
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

sag = StormAnalyticsGenerator()
sag.gen_v_storm_run_membership()

sdb = StormDB()
test_ = test[:10]
test = sdb.get_tracks()
sdb.get_runs_by_storm('film_vg_instrumental')

sac = StormAnalyticsController()

pipeline = {}
pipeline['view_generation_pipeline'] = [('inferred_run_history', {'start':'2008-01-01'}),
                                        ('inferred_storm_run_membership', {'start':'2008-01-01'})]
sac.analytics_pipeline(pipeline)

sac = StormAnalyticsController()
params = {'tracks':[]}
name = 'track_info'
test = sac.gen_view(name, params)

[x.strftime("%Y-%m-%d") for x in pd.date_range('2020-01-07', '2021-01-14', freq='w').tolist()]

fr = FakeRunner('film_vg_instrumental', '2020-01-07', '2020-01-14')
test = fr.Run()
