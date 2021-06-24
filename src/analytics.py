# This file is dedicated to maintaining a MySQL Database of rolled up analytical information 
# from the MongoDB backend. These pipelines are porting over information into tabular formats,
# unnesting documents as necessary. 
#
# The controller executes all the pipelines, which invoke more specific action classes
# The pipelines are setup to execute so long as the mapping in the controllers are up to date
# Updating logic within an action class will without any additional effort update the
# action as it is executed in the pipeline, i.e. central logic source.

import os
from sys import getsizeof
import json
from pymongo import MongoClient
import pandas as pd
import numpy as np
from timeit import default_timer as timer
from tqdm import tqdm
import datetime as dt

from dotenv import load_dotenv
load_dotenv()

from .db import *
from .runner import FakeRunner

class StormAnalyticsGenerator:
    """
    Generates analytical views from the StormDB backend.
    Only connected to StormDB
    Updates made to controller-connected functions will update pipeline funcionality if changed.
    DF's should be returned with properly named Indexes.
    dtypes are contolled here.
    """
    def __init__(self, verbocity=2):
        self.sdb = StormDB()

        # Verbocity
        self.print = print if verbocity > 0 else lambda x: None
        self.tqdm = lambda x: tqdm(x, leave=False) if verbocity > 1 else lambda x: x

    # Playlist Views
    def gen_v_playlist_history(self, playlist_ids=[], index=False):
        """
        Cross-Compares many playlist track changes
        """

        if len(playlist_ids) == 0:
            self.print("No playlists specified, defaulting to all in DB.")
            playlist_ids = self.sdb.get_playlists()
        elif len(playlist_ids) == 1:
            self.print("Only one playlist specified, returning single view.")
            return self.gen_v_single_playlist_history(playlist_ids[0])

        # Generate the multiple view dataframe
        df = pd.DataFrame()
        self.print("Building and combining Playlist views")
        for playlist_id in tqdm(playlist_ids):

            playlist_df = self.gen_v_single_playlist_history(playlist_id, index=False)
            playlist_df['playlist'] = playlist_id

            # Join it back in
            df = pd.concat([df, playlist_df])

        return df.set_index(['date_collected', 'playlist']) if index else df

    def gen_v_single_playlist_history(self, playlist_id, index=False):
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

    def gen_v_playlist_info(self, playlist_ids=[]):
        """
        Reads all static info in for a playlist
        """
        if len(playlist_ids) == 0:
            self.print("No playlists specified, defaulting to all in DB.")
            playlist_ids = self.sdb.get_playlists()

        # Generate the multiple view dataframe
        df = pd.DataFrame(columns=["name", "owner_name", "owner_id", 
                                    "current_snapshot", "description", "last_collected"], 
                         index=playlist_ids)
        self.print("Building and combining Playlist Info")
        for playlist_id in self.tqdm(playlist_ids):

            playlist_data = self.sdb.get_playlist_current_info(playlist_id)
            df.loc[playlist_id, "name"] = playlist_data["info"]["name"]
            df.loc[playlist_id, "owner_name"] = playlist_data["info"]["owner"]["display_name"]
            df.loc[playlist_id, "owner_id"] = playlist_data["info"]["owner"]["id"]
            df.loc[playlist_id, "current_snapshot"] = playlist_data["info"]["snapshot_id"]
            df.loc[playlist_id, "description"] = playlist_data["info"]["description"]
            df.loc[playlist_id, "last_collected"] = playlist_data["last_collected"]

        df.index.rename("playlist", inplace=True)
        return df.reset_index()

    # Run Views
    def gen_v_run_history(self, storm_names=[]):
        """
        Creates a flat table for one or many storm run records.
        """

        if len(storm_names) == 0:
            self.print("No storm names supplied, running it for all.")
            storm_names = self.sdb.get_all_configs() # To be replaced by get_all_config_names

        df = pd.DataFrame()
        self.print(f"Collecting runs for {len(storm_names)} Storms.")
        for storm in storm_names:
            self.print(f"{storm}")
            runs = self.sdb.get_runs_by_storm(storm)

            run_df = pd.DataFrame(index=[x['_id'] for x in runs])
            for run in self.tqdm(runs):

                # Copying
                run_df.loc[run["_id"], 'storm_name'] = storm
                run_df.loc[run['_id'], 'run_date'] = run['run_date']
                run_df.loc[run['_id'], 'start_date'] = run['start_date']
                run_df.loc[run['_id'], 'storm_name'] = storm

                # Direct Aggregations
                agg_keys = ['playlists', 'input_tracks', 'input_artists', 'eligible_tracks', 
                            'storm_tracks', 'storm_artists', 'storm_albums', 'removed_artists', 'removed_tracks',
                            'storm_sample_tracks']

                for key in agg_keys:
                    run_df.loc[run['_id'], f"{key}_cnt"] = len(run[key])

                # Computations
                run_df.loc[run["_id"], 'days'] = (dt.datetime.strptime(run['run_date'], "%Y-%m-%d") - 
                                                  dt.datetime.strptime(run['start_date'], "%Y-%m-%d")).days

            # df Computations
            run_df['storm_tracks_per_artist'] = run_df['storm_tracks_cnt'] / run_df['storm_artists_cnt']
            run_df['storm_tracks_per_day'] = run_df['storm_tracks_cnt'] / run_df['days']
            run_df['storm_tracks_per_artist_day'] = run_df['storm_tracks_per_day'] / run_df['storm_artists_cnt']

            df = pd.concat([df, run_df])

        df.index.rename('run_id', inplace=True)
        return df.reset_index()

    def gen_v_inferred_run_history(self, storm_names=[], start='2020-01-01'):
        """
        Generates hypothetical storms runs from a start date, mostly used for ML
        and for potentially sizing up the impacts of a storm configuration change.
        """
        # Storm Names
        if len(storm_names) == 0:
            self.print("No storm names supplied, running it for all.")
            storm_names = self.sdb.get_all_configs()

        df = pd.DataFrame()
        end = dt.datetime.now().strftime("%Y-%m-%d")
        self.print(f"Generating hypothetical runs for {len(storm_names)} Storms.")
        for storm in storm_names:
            self.print(f"{storm}")

            self.print(f"Generating dates between {start} and {end}")
            dates = [x.strftime("%Y-%m-%d") for x in pd.date_range(start, end, freq='w').tolist()]
            run_df = pd.DataFrame(index=[f'simu_run_{storm}_{"".join(x.split("-"))}' for x in dates])

            for i in self.tqdm(range(len(dates)-1)):

                run = FakeRunner(storm, start_date=dates[i], run_date=dates[i+1], verbocity=0).Run()
                run_id = f'simu_run_{storm}_{"".join(dates[i].split("-"))}'

                # Copying
                run_df.loc[run_id, 'storm_name'] = storm
                run_df.loc[run_id, 'run_date'] = run['run_date']
                run_df.loc[run_id, 'start_date'] = run['start_date']
                run_df.loc[run_id, 'storm_name'] = storm

                # Direct Aggregations
                agg_keys = ['input_artists', 'eligible_tracks', 
                            'storm_tracks', 'storm_artists', 'storm_albums', 'removed_artists', 'removed_tracks',
                            'storm_sample_tracks']

                for key in agg_keys:
                    run_df.loc[run_id, f"{key}_cnt"] = len(run[key])

                # Computations
                run_df.loc[run_id, 'days'] = 7

            # df Computations
            run_df['storm_tracks_per_artist'] = run_df['storm_tracks_cnt'] / run_df['storm_artists_cnt']
            run_df['storm_tracks_per_day'] = run_df['storm_tracks_cnt'] / run_df['days']
            run_df['storm_tracks_per_artist_day'] = run_df['storm_tracks_per_day'] / run_df['storm_artists_cnt']

            df = pd.concat([df, run_df])

        df.index.rename('run_id', inplace=True)
        return df.reset_index()
        

    # Track Views
    def gen_v_track_info(self, tracks=[]):
        """
        Essentially a copy and paste of the tracks in the DB
        """
        
        if len(tracks) == 0:
            self.print("No tracks supplied, running it for all.")
            tracks = self.sdb.get_tracks()

        df = pd.DataFrame(self.sdb.get_track_info(tracks))
        df.rename(columns={'_id':'track'})

        # Release Date
        album_release_dates = self.sdb.get_album_info(df.album_id.unique().tolist(), fields={"_id":1, "release_date":1})
        release_date_dict = {}

        for album in album_release_dates:
            release_date_dict[album['_id']] = album['release_date']

        df['release_date'] = df.album_id.apply(lambda x: release_date_dict[x])

        return df

    def gen_v_storm_target_membership(self, storm_names=[], target_group='good'):
        """
        Generates a list of tracks that meet target group for particular storm
        """
        if len(storm_names) == 0:
            self.print("No storm names supplied, running it for all.")
            storm_names = self.sdb.get_all_configs()

        df = pd.DataFrame(columns=['track_id', 'storm_name', 'target_group'])
        for storm in self.tqdm(storm_names):
            config = self.sdb.get_config(storm)
            storm_df = pd.DataFrame(columns=['track_id', 'storm_name', 'target_group'])
            
            if target_group in ['good', 'all']:

                # Generate view for good playlist
                good = config['good_targets']
                tracks = self.sdb.get_loaded_playlist_tracks(good)

                temp = pd.DataFrame(tracks, index=[x for x in range(len(tracks))], columns=['track_id'])
                temp['target_group'] = 'good'
                storm_df = pd.concat([storm_df, temp])

            if target_group in ['great', 'all']:

                # Generate view for great playlist
                great = config['great_targets']
                tracks = self.sdb.get_loaded_playlist_tracks(great)

                temp = pd.DataFrame(tracks, index=[x for x in range(len(tracks))], columns=['track_id'])
                temp['target_group'] = 'great'
                storm_df = pd.concat([storm_df, temp])

            storm_df['storm_name'] = storm
            df = pd.concat([df, storm_df])
        
        return df

    def gen_v_storm_run_membership(self, storm_names=[]):
        """
        Tracks by storm and date they were included in a storm run.
        This uses the run_records storm_tracks field.
        """
        if len(storm_names) == 0:
            self.print("No storm names supplied, running it for all.")
            storm_names = self.sdb.get_all_configs()

        df = pd.DataFrame(columns=['track_id', 'run_id'])
        for storm in self.tqdm(storm_names):
            self.print(f"{storm}")
            runs = self.sdb.get_runs_by_storm(storm)
            storm_df = pd.DataFrame(columns=['track_id', "run_id"])

            for run in self.tqdm(runs):

                tracks = run['storm_tracks']
                run_df = pd.DataFrame(index=tracks, columns=["run_id"])
                run_df["run_id"] = run["_id"]
                
                
                run_df.index.rename('track_id', inplace=True)
                run_df.reset_index(inplace=True)
                storm_df = pd.concat([storm_df, run_df])


            df = pd.concat([df, storm_df])

        return df

    def gen_v_inferred_storm_run_membership(self, storm_names=[], start='2020-01-01'):
        """
        Tracks by storm and date they were included in a hypoethetical storm_run.
        Since this a theoretical run it lives entirely within the SADB.
        """

        # Storms
        if len(storm_names) == 0:
            self.print("No storm names supplied, running it for all.")
            storm_names = self.sdb.get_all_configs()

        end = dt.datetime.now().strftime("%Y-%m-%d")
        df = pd.DataFrame(columns=['track_id', 'run_id'])
        for storm in self.tqdm(storm_names):
            self.print(f"{storm}")
            runs = self.sdb.get_runs_by_storm(storm)
            storm_df = pd.DataFrame(columns=['track_id', "run_id"])

            self.print(f"Generating dates between {start} and {end}")
            dates = [x.strftime("%Y-%m-%d") for x in pd.date_range(start, end, freq='w').tolist()]
            run_df = pd.DataFrame(index=[f'simu_run_{storm}_{"".join(x.split("-"))}' for x in dates])

            for i in self.tqdm(range(len(dates)-1)):

                run = FakeRunner(storm, start_date=dates[i], run_date=dates[i+1], verbocity=0).Run()
                run_id = f'simu_run_{storm}_{"".join(dates[i].split("-"))}'

                tracks = run['storm_tracks']
                run_df = pd.DataFrame(index=tracks, columns=["run_id"])
                run_df["run_id"] = run_id
                
                
                run_df.index.rename('track_id', inplace=True)
                run_df.reset_index(inplace=True)
                storm_df = pd.concat([storm_df, run_df])


            df = pd.concat([df, storm_df])

        return df

        
    # ML Views
    def gen_ml_v_storm_tracks(self, storm_names=[]):
        """
        Tracks by storm and date that could've been listened to.
        These are actuals, meaning these tracks must have moved through a storm
        See gen_ml_v_storm_tracks_inferred for a view that is not actuals, 
        but hypothetical storm tracks in a date range.
        """
        if len(storm_names) == 0:
            self.print("No storm names supplied, running it for all.")
            storm_names = self.sdb.get_all_configs()

    def gen_ml_v_storm_tracks_inferred(self, start_at='2020-01-01', end_at='2021-05-01', storm_names=[]):
        """
        Uses track release dates to build hypothetical historical storm tracks
        and then writes a view identical to gen_ml_v_storm_tracks for that
        hypothetical track list. This allows for a set of targets to be built
        in the same fashion that an ongiong training loop would have.
        """
        return None


class StormAnalyticsController:
    """
    Wraps around a StormDB (Mongo backend) and a StormAnalyticsDB (MySQL analytics DB) to generate 
    and write out different analytical views.
    Connected to a generator, writer and database. Main orchestration tool
    """

    def __init__(self, verbocity=3):

        # Connections
        self.sdb = StormDB()
        self.sadb = StormAnalyticsDB()
        self.sag = StormAnalyticsGenerator(verbocity=verbocity-1)

        # All of the available views that could be written to SADB. Supply Params on invocation
        self.view_map = {'single_playlist_history':self.sag.gen_v_single_playlist_history,
                             'playlist_history':self.sag.gen_v_playlist_history,                                                 
                             'playlist_info':self.sag.gen_v_playlist_info,
                             'run_history':self.sag.gen_v_run_history,
                             'track_info':self.sag.gen_v_track_info,
                             'storm_target_membership':self.sag.gen_v_storm_target_membership,
                             'storm_run_membership':self.sag.gen_v_storm_run_membership,
                             'inferred_run_history':self.sag.gen_v_inferred_run_history,
                             'inferred_storm_run_membership':self.sag.gen_v_inferred_storm_run_membership}
        
        # Verbocity
        self.print = print if verbocity > 0 else lambda x: None
        self.tqdm = lambda x: tqdm(x, leave=False) if verbocity > 1 else lambda x: x

    def analytics_pipeline(self, custom_pipeline=None):
        """
        Complete Orchestration function combining SDB -> SADB, SADB -> SADB and SADB -> SMLDB
        for all storm runs.
        Can run a custom pipeline, which is a dict containing the following pipelines:
            - view_generation_pipeline (SDB -> SADB)
            - view_aggregation_pipeline (SADB -> SADB)
            - machine_learning_input_pipeline (SADB -> SMLDB)
            - machine_learning_output_pipeline (SMLDB -> SADB)
        """

        if custom_pipeline is None:
            # Default orchestration (aka the entire database)
            pipeline = {}

            # SDB -> SADB
            pipeline['view_generation_pipeline'] = [('playlist_history', {"playlist_ids":[]}),
                                                    ('playlist_info', {"playlist_ids":[]}),
                                                    ('run_history', {"storm_names":[]}),
                                                    ('track_info', {"tracks":[]})]

            # SADB -> SADB
            pipeline['view_aggregation_pipeline'] = []

        else:
            pipeline = custom_pipeline

        start = timer()
        self.print("Executing Pipelines . . .\n")
        [self.write_view(task[0], self.gen_view(task[0], task[1])) for task in pipeline['view_generation_pipeline']]
        end = timer()
        self.print("Pipelines Complete!")
        self.print(f"Elapsed Time: {round(end-start, 4)}s \n")

    # Generic generate and write views
    def gen_view(self, name, view_params={}):
        """
        Caller function for views (prints and other nice additions)
        """
        if name in self.view_map.keys():
            self.print(f"Generating View: {name}")
            self.print(f"Supplied Parameters: {view_params}")

            start = timer()
            r = self.view_map[name](**view_params)
            end = timer()

            self.print("View Complete!")
            self.print(f"Elapsed Time to Build: {round(end-start, 4)}s | File Size: {getsizeof(r)} bytes\n")

            return r

        else:
            raise Exception(f"View {name} not in map.")

    def write_view(self, name, data, **kwargs):
        """
        Function for writing a view
        """
        self.print(f"Writing {name} to SADB.")

        start = timer()
        self.sadb.write_table(name, data, **kwargs)
        end = timer()

        self.print("View Written!")
        self.print(f"Elapsed Time to Write: {round(end-start, 4)}s \n")

    def sql_to_sql(self, query_name, query_params={}, processing=False, **kwargs):
        """
        Reads in a query and writes the data back out. 
        Can do non-sql processing in between.
        """
        start = timer()
        self.print("Reading . . .")
        df = self.sadb.read_table(q=open(f'queries/{query_name}.sql', 'r').read().format(**query_params))

        self.print("Processing . . .")
        if processing:
            self.print("do processing.")

        self.print("Writing . . .")
        self.write_view(query_name, df)
        end = timer()

        self.print("View Written!")
        self.print(f"Elapsed Time to Write: {round(end-start, 4)}s \n")

    