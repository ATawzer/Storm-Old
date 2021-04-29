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

# Internal
from src.db import *


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