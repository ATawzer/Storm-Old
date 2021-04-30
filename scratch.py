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

Storm(['contemporary_lyrical']).Run()



sdb = StormDB()
sdb.get_runs_by_storm('film_vg_instrumental')

sac = StormAnalyticsController()
sac.analytics_pipeline()

pipeline = {}
pipeline['view_generation_pipeline'] = [('playlist_info', {"playlist_ids":[]}),
                                                    ('run_history', {"storm_names":[]})]
sac.analytics_pipeline(pipeline)

sac = StormAnalyticsController()
params = {'storm_names':[]}
name = 'run_history'
test = sac.gen_view(name, params)