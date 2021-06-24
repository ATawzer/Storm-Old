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
from src.weatherboy import *
from src.storm import Storm

StormDB().dedup_tracks_on_name()

Storm(['contemporary_lyrical', 'film_vg_instrumental']).Run()

fr = FakeRunner('film_vg_instrumental', '2020-01-07', '2020-01-14')
test = fr.Run()

wbp = WeatherBoyPipeline(test['storm_tracks'], 
                        'film_vg_instrumental',
                         features={'_id':0, 
                                   'album_id':0, 
                                   'last_updated':0,
                                   'name':0,
                                   'time_signature':0,
                                   'artists':0},
                        train=True)
wbp.load()

wbm = WeatherBoyModel(hyper_params={'is_unbalance':True}, train=True)
wbm.fit(wbp)

