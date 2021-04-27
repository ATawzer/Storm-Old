import os
from sys import getsizeof
import json
from pymongo import MongoClient
import pandas as pd
import numpy as np
from timeit import default_timer as timer

from dotenv import load_dotenv
load_dotenv()

from .db import *

class StormAnalyticsGenerator:
    """
    Generates analytical views from the StormDB backend.
    Only connected to StormDB
    """
    def __init__(self):
        self.sdb = StormDB()

class StormAnalyticsWriter:
    """
    Writes views into the MySQL endpoint
    Only connected to the analytics database
    """
    def __init__(self):
        self.sadb = StormAnalyticsDB()

class StormAnalyticsController:
    """
    Wraps around a StormDB (Mongo backend) and a StormAnalyticsDB (MySQL analytics DB) to generate 
    and write out different analytical views.
    Connected to a generator, writer and database. Main orchestration tool
    """

    def __init__(self, verbose=True):

        self.sdb = StormDB()
        self.sadb = StormAnalyticsDB()
        self.sag = StormAnalyticsGenerator()
        self.saw = StormAnalyticsWriter()

        self.view_gen_map = {'playlist_track_changes':self.gen_v_playlist_track_changes,
                         'many_playlist_track_changes':self.gen_v_many_playlist_track_changes}
        self.view_write_map = {}
        self.print = print if verbose else lambda x: None

    # Generic generate and write views
    def gen_view(self, name, view_params={}):
        """
        Caller function for views (prints and other nice additions)
        """
        if name in self.map.keys():
            self.print(f"Generating View: {name}")
            self.print(f"Supplied Parameters: {view_params}")

            start = timer()
            r = self.map[name](**view_params)
            end = timer()

            self.print("View Complete!")
            self.print(f"Elapsed Time to Build: {round(end-start, 4)} ms. | File Size: {getsizeof(r)} bytes")

            return r

        else:
            raise Exception(f"View {name} not in map.")

    def save_view(self, result)

    def gen_v_many_playlist_track_changes(self, playlist_ids=[], index=False):
        """
        Cross-Compares many playlist track changes
        """

        if len(playlist_ids) == 0:
            self.print("No playlists specified, defaulting to all in DB.")
            playlist_ids = self.sdb.get_playlists()
        elif len(playlist_ids) == 1:
            self.print("Only one playlist specified, returning single view.")
            return self.gen_v_playlist_track_changes(playlist_ids[0])

        # Generate the multiple view dataframe
        df = pd.DataFrame()
        self.print("Building and combining Playlist views")
        for playlist_id in tqdm(playlist_ids):

            playlist_df = self.gen_v_playlist_track_changes(playlist_id, index=False)
            playlist_df['playlist'] = playlist_id

            # Join it back in
            df = pd.concat([df, playlist_df])

        return df.set_index(['date_collected', 'playlist']) if index else df

    # Single object views - low-level
    def gen_v_playlist_track_changes(self, playlist_id, index=False):
        """
        Generates a view of a playlists timely health
        """

        #playlist_info = self.sdb.get_playlist_current_info()
        playlist_changelog = self.sdb.get_playlist_changelog(playlist_id)

        # Create Dataframe
        df = pd.DataFrame(index=list(playlist_changelog.keys()))

        # Compute Metrics
        for change in playlist_changelog:

            # Tracks
            df.loc[change, 'Number of tracks'] = len(playlist_changelog[change]['tracks'])

            # Artists
            artists = []
            [artists.extend(self.sdb.get_track_artists(x)) for x in playlist_changelog[change]['tracks']]
            df.loc[change, 'Number of Artists'] = len(np.unique(artists))

        # Metadata
        df.index.rename('date_collected', inplace=True)

        return df if index else df.reset_index()