# Modeling
import numpy as np
import pandas as pd
from tqdm import tqdm

from dotenv import load_dotenv
load_dotenv() 

# Directory writing
import os

# Internal
from .db import *

class WeatherBoy:

    def __init__(self, sdb):

        self.sdb = sdb

    def rank_tracks(self, tracks, good_playlist, great_playlist):
        """
        Ranks storm tracks
        """

        track_info = self.sdb.get_track_info(tracks, fields={'artists':1, 'album_id':1})
        good_tracks = self.sdb.get_loaded_playlist_tracks(good_playlist)
        great_tracks = self.sdb.get_loaded_playlist_tracks(great_playlist)    

        good_artists = np.array([artist for track in self.sdb.get_track_info(good_tracks, fields={'artists':1}) for artist in track['artists']])
        great_artists = np.array([artist for track in self.sdb.get_track_info(great_tracks, fields={'artists':1}) for artist in track['artists']])

        # Ranks according to presence
        for track in tqdm(track_info):
            track['artist_count'] = np.mean([np.count_nonzero(good_artists==x) for x in track['artists']]) + \
                                            (10*np.mean([np.count_nonzero(great_artists==x) for x in track['artists']]))

        df = pd.DataFrame(track_info).sort_values(['artist_count', 'album_id'], ascending=False)
        return df['_id'].tolist()