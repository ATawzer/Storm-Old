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
sdb.get_playlists(name=True)

sadb = StormAnalyticsDB()
params = {'playlist_id':'0R1gw1JbcOFD0r8IzrbtYP', 'index':True}
name = 'playlist_track_changes'
test = sadb.gen_view(name, params)


params = {'playlist_ids':[], 'index':True}
name = 'many_playlist_track_changes'
test = sadb.gen_view(name, params)