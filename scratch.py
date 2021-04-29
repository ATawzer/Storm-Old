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

pipeline = {}
pipeline['view_generation_pipeline'] = [('playlist_info', {"playlist_ids":[]})]
sac.analytics_pipeline(pipeline)

sag = StormAnalyticsGenerator()
params = {'playlist_ids':[], 'index':True}
name = 'many_playlist_track_changes'
test = sadb.gen_view(name, params)