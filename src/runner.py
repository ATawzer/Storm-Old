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

# DB
from .db import *
from .storm_client import *
from .weatherboy import *
from pymongo import MongoClient

class FakeRunner:
    """
    Orchestrates a non-recorded, no API storm run given start and end_dates.
    Returns the hypothetical run_record gathered from taking the most recent run from
    that storms artists
    """
    def __init__(self, storm_name, start_date, run_date, verbocity=1):

        print(f"Initializing Runner for {storm_name}")
        self.sdb = StormDB()
        self.config = self.sdb.get_config(storm_name)
        self.name = storm_name
        self.start_date = start_date
        self.run_date = run_date

        # Verbocity
        self.print = print if verbocity > 0 else lambda x: None
        self.tqdm = lambda x: tqdm(x, leave=False) if verbocity > 1 else lambda x: x

        # metadata
        self.run_record = {'config':self.config, 
                           'storm_name':self.name,
                           'run_date':self.run_date,
                           'start_date':self.start_date,
                           'input_artists':[], # Determines what gets collected, also 'egligible' artists
                           'eligible_tracks':[], # Tracks that could be delivered before track filters
                           'storm_tracks':[], # Tracks actually written out
                           'storm_artists':[], # Used for track filtering
                           'storm_albums':[], # Release Date Filter
                           'storm_sample_tracks':[], # subset of storm tracks delivered to sample
                           'removed_artists':[] # Artists filtered out
                           }

        self.print(f"{self.name} Started Successfully!\n")
        #self.Run()

    def Run(self):
        """
        Storm Orchestration based on a configuration.
        """

        self.print(f"{self.name} - Step 1 / 2 - Getting Storm Artists from most recent run . . .")
        self.load_last_run()

        self.print(f"{self.name} - Step 2 / 2 - Filtering Track List . . . ")
        self.filter_storm_tracks()

        self.print(f"{self.name} - Complete!\n")
        return self.run_record
    
    def load_last_run(self):
        """
        Loads in relevant information from last run.
        """
        self.print("Appending last runs tracks and artists.")
        self.run_record['input_artists'].extend(self.sdb.get_last_run(self.name)['storm_artists']) # Post-filter

    def filter_storm_tracks(self):
        """
        Get a List of tracks to deliver.
        """

        self.print("Filtering artists.")
        self.apply_artist_filters()

        self.print("Obtaining all albums from storm artists.")
        self.run_record['storm_albums'] = self.sdb.get_albums_from_artists_by_date(self.run_record['storm_artists'], 
                                                                                   self.run_record['start_date'],
                                                                                   self.run_date)
        self.print("Getting tracks from albums.")
        self.run_record['eligible_tracks'] = self.sdb.get_tracks_from_albums(self.run_record['storm_albums'])

        self.print("Filtering Tracks.")
        self.apply_track_filters()

        self.print("Storm Tracks Generated! \n")

    def apply_artist_filters(self):
        """
        read in filters from configurations
        """
        filters = self.config['filters']['artist']
        supported = ['genre', 'blacklist']
        bad_artists = []

        # Filters
        self.print(f"{len(filters)} valid filters to apply")
        for filter_name, filter_value in filters.items():
            
            self.print(f"Attemping filter {filter_name} - {filter_value}")
            if filter_name == 'genre':
                # Add all known artists in sdb of a genre to remove in tracks later
                genre_artists = self.sdb.get_artists_by_genres(filter_value)
                bad_artists.extend(genre_artists)

            elif filter_name == 'blacklist':
                blacklist = self.sdb.get_blacklist(filter_value)
                if len(blacklist) == 0:
                    self.print(f"{filter_value} not found, no filtering will be done.'")
                else:
                    bad_artists.extend(blacklist[0]['blacklist'])
            else:
                self.print(f"{filter_name} not supported or misspelled. ")

        self.run_record['storm_artists'] = [x for x in self.run_record['input_artists'] if x not in bad_artists]
        self.run_record['removed_artists'] = bad_artists
        self.print(f"Starting Artist Amount: {len(self.run_record['input_artists'])}")
        self.print(f"Ending Artist Amount: {len(self.run_record['storm_artists'])}")

    def apply_track_filters(self):
        """
        read in filters from configurations
        """
        filters = self.config['filters']['track']
        supported = ['audio_features', 'artist_filter']
        bad_tracks = []

        # Filters
        self.print(f"{len(filters)} valid filters to apply")
        for filter_name, filter_value in filters.items():
            
            self.print(f"Attemping filter {filter_name} - {filter_value}")
            if filter_name == 'audio_features':
                for feature, feature_value in filter_value.items():
                    op = f"${feature_value.split('&&')[0]}"
                    val = float(feature_value.split('&&')[1])
                    self.print(f"Removing tracks with {feature} - {op}:{val}")
                    valid = self.sdb.filter_tracks_by_audio_feature(self.run_record['eligible_tracks'], {feature:{op:val}})
                    bad_tracks.extend([x for x in self.run_record['eligible_tracks'] if x not in valid])
                    self.print(f"Cumulative Bad Tracks found {len(np.unique(bad_tracks))}")

                
            elif filter_name == "artist_filter":
                if filter_value == 'hard':
                    # Limits output to tracks that contain only storm artists
                    for track in tqdm(self.run_record['eligible_tracks']):

                        track_artists = set(self.sdb.get_track_artists(track))
                        if not track_artists.issubset(set(self.run_record['storm_artists'])):
                            bad_tracks.append(track)

                elif filter_value == 'soft':
                    # Removes tracks that contain known filtered out artists
                    # Other 'bad' artists could sneak in if not tracked by storm
                    for track in tqdm(self.run_record['eligible_tracks']):
                        track_artists = set(self.sdb.get_track_artists(track))
                        if not set(self.run_record['removed_artists']).isdisjoint(track_artists):
                            bad_tracks.append(track)

            else:
                self.print(f"{filter_name} not supported or misspelled. ")

        bad_tracks = np.unique(bad_tracks).tolist()
        self.print("Removing bad tracks . . .")
        self.run_record['storm_tracks'] = [x for x in self.run_record['eligible_tracks'] if x not in bad_tracks]
        self.run_record['removed_tracks'] = bad_tracks
        self.print(f"Starting Track Amount: {len(self.run_record['eligible_tracks'])}")
        self.print(f"Ending Track Amount: {len(self.run_record['storm_tracks'])}")


class StormRunner:
    """
    Orchestrates a storm run
    """
    def __init__(self, storm_name, start_date=None, ignore_rerelease=True):

        print(f"Initializing Runner for {storm_name}")
        self.sdb = StormDB()
        self.config = self.sdb.get_config(storm_name)
        self.sc = StormClient(self.config['user_id'])
        self.suc = StormUserClient(self.config['user_id'])
        self.name = storm_name
        self.start_date = start_date
        self.wb = WeatherBoy(self.sdb)
        self.ignore_rerelease = ignore_rerelease

        # metadata
        self.run_date = dt.datetime.now().strftime('%Y-%m-%d')
        self.run_record = {'config':self.config, 
                           'storm_name':self.name,
                           'run_date':self.run_date,
                           'start_date':self.start_date,
                           'playlists':[],
                           'input_tracks':[], # Determines what gets collected
                           'input_artists':[], # Determines what gets collected, also 'egligible' artists
                           'eligible_tracks':[], # Tracks that could be delivered before track filters
                           'storm_tracks':[], # Tracks actually written out
                           'storm_artists':[], # Used for track filtering
                           'storm_albums':[], # Release Date Filter
                           'storm_sample_tracks':[], # subset of storm tracks delivered to sample
                           'removed_artists':[] # Artists filtered out
                           }
        self.last_run = self.sdb.get_last_run(self.name)
        self.gen_dates()

        print(f"{self.name} Started Successfully!\n")
        #self.Run()

    def Run(self):
        """
        Storm Orchestration based on a configuration.
        """

        print(f"{self.name} - Step 0 / 8 - Initializing using last run.")
        self.load_last_run()

        print(f"{self.name} - Step 1 / 8 - Collecting Playlist Tracks and Artists. . .")
        self.collect_playlist_info()
        
        print(f"{self.name} - Step 2 / 8 - Collecting Artist info. . .")
        self.collect_artist_info()

        print(f"{self.name} - Step 3 / 8 - Collecting Albums and their Tracks. . .")
        self.collect_album_info()

        print(f"{self.name} - Step 4 / 8 - Collecting Track Features . . .")
        self.collect_track_features()

        print(f"{self.name} - Step 5 / 8 - Filtering Track List . . .")
        self.filter_storm_tracks()

        print(f"{self.name} - Step 6 / 8 - Handing off to Weatherboy . . . ")
        self.call_weatherboy()

        print(f"{self.name} - Step 7 / 8 - Writing to Spotify . . .")
        self.write_storm_tracks()

        print(f"{self.name} - Step 8 / 8 - Saving Storm Run . . .")
        self.save_run_record()

        print(f"{self.name} - Complete!\n")
    
    # Object Based orchestration
    def load_last_run(self):
        """
        Loads in relevant information from last run.
        """

        if self.last_run is None:
            print("Storm is new, nothing to load")

        else:
            print("Appending last runs tracks and artists.")
            self.run_record['input_tracks'].extend(self.last_run['input_tracks'])
            self.run_record['input_artists'].extend(self.last_run['storm_artists']) # Post-filter

    def collect_playlist_info(self):
        """
        Initial Playlist setup orchestration
        """

        print("Loading Great Targets . . .")
        self.load_playlist(self.config['great_targets'])

        print("Loading Good Targets . . .")
        self.load_playlist(self.config['good_targets'])

        # Check for additional playlists
        if 'additional_input_playlists' in self.config.keys():
            if self.config['additional_input_playlists']['is_active']:
                for ap, ap_id in self.config['additional_input_playlists']['playlists'].items():
                    print(f"Loading Additional Playlist: {ap}")
                    self.load_playlist(ap_id)
        
        # Check what songs remain in sample and full delivery
        self.load_output_playlist(self.config['full_storm_delivery']['playlist'])

        ## ---- Future Version ----
        self.load_output_playlist(self.config['rolling_good']['playlist'])
        # Check if we need to move rolling
       
        print("Playlists Prepared. \n")

    def collect_artist_info(self):
        """
        Loads in the data from the run_records artists
        """

        # get data for artists we don't know
        known_artists = self.sdb.get_known_artist_ids()
        new_artists = [x for x in self.run_record['input_artists'] if x not in known_artists]

        if len(new_artists) > 0:
            print(f"{len(new_artists)} New Artists Found! Getting their info now.")
            new_artist_info = self.sc.get_artist_info(new_artists)

            print("Writing their info to DB . . .")
            self.sdb.update_artists(new_artist_info)
        
        else:
            print("No new Artists found.")

        print("Artist Info Collection Done.\n")

    def collect_album_info(self):
        """
        Get and update all albums associated with the artists
        """
        
        print("Getting the albums for Input Artists that haven't been acquired.")
        self.collect_artist_albums()
        
        print("Getting tracks for albums that need it")
        self.collect_album_tracks()
    
        print("Album Collection Done. \n")

    def collect_track_features(self):
        """
        Gets all track features needed
        Also in a while try except loop to get through all tracks in the case of bad batches.
        """
        
        to_collect = self.sdb.get_tracks_for_feature_collection()
        if len(to_collect) == 0:
            print("No Track Features to collect.")
            return True

        batch_size = 1000
        batches = np.array_split(to_collect, int(np.ceil(len(to_collect)/batch_size)))

        # Attempt to go get the batches
        bad_batch_retries = 0
        consecutive_bad_batches_limit = 10
        retry_limit = 5
        while (bad_batch_retries < retry_limit) & (len(batches) > 0):

            bad_batches = []
            consecutive_bad_batches = 0
            print(f"Batch Size: {batch_size} | Number of Batches {len(batches)}")
            for batch in tqdm(batches):

                if consecutive_bad_batches > consecutive_bad_batches_limit:
                    raise Exception(f"{consecutive_bad_batches_limit} consecutive bad batches. . . Terminating Process.")
                try:
                    batch_tracks = self.sc.get_track_features(batch)
                    self.sdb.update_track_features(batch_tracks)

                    # Successful, does not need collection
                    consecutive_bad_batches = 0

                except:
                    print("Bad Batch, will try again after.")
                    bad_batches.append(batch)
                    consecutive_bad_batches += 1

            bad_batch_retries += 1
            batches = bad_batches

            bad_batch_retries += 1
        
        print("All Track batches collected!")
        print("Track Collection Done! \n")
        return True

    def filter_storm_tracks(self):
        """
        Get a List of tracks to deliver.
        """

        print("Filtering artists.")
        self.apply_artist_filters()

        print("Obtaining all albums from storm artists.")
        self.run_record['storm_albums'] = self.sdb.get_albums_from_artists_by_date(self.run_record['storm_artists'], 
                                                                                   self.run_record['start_date'],
                                                                                   self.run_date)
        print("Getting tracks from albums.")
        self.run_record['eligible_tracks'] = self.sdb.get_tracks_from_albums(self.run_record['storm_albums'])

        print("Filtering Tracks.")
        self.apply_track_filters()

        if self.ignore_rerelease:
            print("Handling Duplicates and Previously Delivered.")
            self.filter_rereleases()

        print("Storm Tracks Generated! \n")

    def call_weatherboy(self):
        """
        Run Modeling process
        """

        self.run_record['storm_tracks'] = self.wb.rank_tracks(self.run_record['storm_tracks'], self.config['good_targets'], self.config['great_targets'])

    def write_storm_tracks(self):
        """
        Output the tracks in storm_tracks
        """

        self.suc.write_playlist_tracks(self.config['full_storm_delivery']['playlist'], self.run_record['storm_tracks'])

    def save_run_record(self):
        """
        Update Metadata and save run_record
        """
        self.sdb.write_run_record(self.run_record)


    # Low Level orchestration
    def gen_dates(self):
        """
        If there was a last run, do all tracks in between. Otherwise do a week since run
        """

        if self.last_run is not None:
            if 'run_date' in self.last_run.keys():
                #last_run_date = dt.strptime(self.last_run['run_date'], "%Y-%m-%d")
                self.start_date = self.last_run['run_date'] if self.start_date is None else self.start_date
                self.run_record['start_date'] = self.start_date
        
        if self.start_date is None:
            self.start_date = (dt.datetime.now() - dt.timedelta(days=7)).strftime("%Y-%m-%d")
            self.run_record['start_date'] = self.start_date

    def load_playlist(self, playlist_id):
        """
        Pulls down playlist info and writes it back to db
        """

        # Determine if playlists need examining
        if self.run_date > self.sdb.get_playlist_collection_date(playlist_id):

            # Acquire data
            playlist_record = {'_id':playlist_id, 
                            'last_collected':self.run_date}

            playlist_record['info'] = self.sc.get_playlist_info(playlist_id)
            playlist_record['tracks'] = self.sc.get_playlist_tracks(playlist_id)
            playlist_record['artists'] = self.sc.get_artists_from_tracks(playlist_record['tracks'])

            print("Writing changes to DB")
            self.sdb.update_playlist(playlist_record)

        else:
            print("Skipping API Load, already collected today.")

        # Get the playlists tracks from DB
        input_tracks = self.sdb.get_loaded_playlist_tracks(playlist_id)
        input_artists = self.sdb.get_loaded_playlist_artists(playlist_id)

        # Update run record
        self.run_record['playlists'].append(playlist_id)
        self.run_record['input_tracks'].extend([x for x in input_tracks if x not in self.run_record['input_tracks']])
        self.run_record['input_artists'].extend([x for x in input_artists if x not in self.run_record['input_artists']])

    def load_output_playlist(self, playlist_id):
        """
        Pulls down playlist info and writes it back to db
        """

        # Determine if playlists need examining
        if self.run_date > self.sdb.get_playlist_collection_date(playlist_id):

            # Acquire data
            playlist_record = {'_id':playlist_id, 
                            'last_collected':self.run_date}

            playlist_record['info'] = self.sc.get_playlist_info(playlist_id)
            playlist_record['tracks'] = self.sc.get_playlist_tracks(playlist_id)
            if len(playlist_record['tracks']) > 0:
                playlist_record['artists'] = self.sc.get_artists_from_tracks(playlist_record['tracks'])

                print("Writing changes to DB")
                self.sdb.update_playlist(playlist_record)
            else:
                print("No tracks, must be new storm or something odd is happening.")

        else:
            print("Skipping API Load, already collected today.")

    def load_artist_albums(self, artists):
        """
        Get many artists information in batches and write back to database incrementally.
        """
        batch_size = 20
        batches = np.array_split(artists, int(np.ceil(len(artists)/batch_size)))

        print(f"Batch Size: {batch_size} | Number of Batches {len(batches)}")
        for batch in tqdm(batches):

            batch_albums = self.sc.get_artist_albums(batch)
            self.sdb.update_albums(batch_albums)
            self.sdb.update_artist_album_collected_date(batch)

    def collect_artist_albums(self):
        """
        Get artist albums for input artists that need it.
        """
        # Get a list of all artists in storm that need album collection
        needs_collection = self.sdb.get_artists_for_album_collection(self.run_date)
        to_collect = [x for x in self.run_record['input_artists'] if x in needs_collection]

        # Get their albums
        if len(to_collect) == 0:
            print("Evey Input Artist's Albums already acquired today.")
        else:
            print(f"New albums to collect for {len(to_collect)} artists.")
            print("Collecting data in batches from API and Updating DB.")
            self.load_artist_albums(to_collect)

        print("Updating artist album association in DB.")
        self.sdb.update_artist_albums()

    def collect_album_tracks(self):
        """
        Gets tracks for every album that needs them, not just storm.
        In the case of new storms this helps populate historical.
        In the case of existing ones it will only be the storm albums that need collection.
        Given the intensity, try except implemented to retry bad batches
        """
        needs_collection = self.sdb.get_albums_for_track_collection()
        batch_size = 20
        if len(needs_collection) == 0:
            print("No Albums needed to collect.")
            return True

        batches = np.array_split(needs_collection, int(np.ceil(len(needs_collection)/batch_size)))

        # Attempt to go get the batches
        bad_batch_retries = 0
        consecutive_bad_batches_limit = 10
        retry_limit = 5
        while (bad_batch_retries < retry_limit) & (len(batches) > 0):

            bad_batches = []
            consecutive_bad_batches = 0
            print(f"Batch Size: {batch_size} | Number of Batches {len(batches)}")
            for batch in tqdm(batches):

                if consecutive_bad_batches > consecutive_bad_batches_limit:
                    raise Exception(f"{consecutive_bad_batches_limit} consecutive bad batches. . . Terminating Process.")
                try:
                    batch_tracks = self.sc.get_album_tracks(batch)
                    self.sdb.update_tracks(batch_tracks)

                    # Successful, does not need collection
                    consecutive_bad_batches = 0

                except:
                    print("Bad Batch, will try again after.")
                    bad_batches.append(batch)
                    consecutive_bad_batches += 1

            bad_batch_retries += 1
            batches = bad_batches
        
        print("All album batches collected!")
        return True

    def apply_artist_filters(self):
        """
        read in filters from configurations
        """
        filters = self.config['filters']['artist']
        supported = ['genre', 'blacklist']
        bad_artists = []

        # Filters
        print(f"{len(filters)} valid filters to apply")
        for filter_name, filter_value in filters.items():
            
            print(f"Attemping filter {filter_name} - {filter_value}")
            if filter_name == 'genre':
                # Add all known artists in sdb of a genre to remove in tracks later
                genre_artists = self.sdb.get_artists_by_genres(filter_value)
                bad_artists.extend(genre_artists)

            elif filter_name == 'blacklist':
                blacklist = self.sdb.get_blacklist(filter_value)
                if len(blacklist) == 0:
                    print(f"{filter_value} not found, no filtering will be done.'")
                else:
                    print(f"{filter_value} found!'")
                    if 'input_playlist' in blacklist[0].keys():
                        print("Updating Blacklist . . .")
                        self.update_blacklist_from_playlist(blacklist[0]['_id'], blacklist[0]['input_playlist'])

                        # Reload
                        blacklist = self.sdb.get_blacklist(filter_value)
                    bad_artists.extend(blacklist[0]['blacklist'])
            else:
                print(f"{filter_name} not supported or misspelled. ")

        self.run_record['storm_artists'] = [x for x in self.run_record['input_artists'] if x not in bad_artists]
        self.run_record['removed_artists'] = bad_artists
        print(f"Starting Artist Amount: {len(self.run_record['input_artists'])}")
        print(f"Ending Artist Amount: {len(self.run_record['storm_artists'])}")

    def update_blacklist_from_playlist(self, blacklist_name, playlist_id):
        """
        Updates a blacklist from a playlist (reads the artists)
        """
        bl_tracks = self.sc.get_playlist_tracks(playlist_id)
        bl_artists = self.sc.get_artists_from_tracks(bl_tracks)
        self.sdb.update_blacklist(blacklist_name, bl_artists)

    def apply_track_filters(self):
        """
        read in filters from configurations
        """
        filters = self.config['filters']['track']
        supported = ['audio_features', 'artist_filter']
        bad_tracks = []

        # Filters
        print(f"{len(filters)} valid filters to apply")
        for filter_name, filter_value in filters.items():
            
            print(f"Attemping filter {filter_name} - {filter_value}")
            if filter_name == 'audio_features':
                for feature, feature_value in filter_value.items():
                    op = f"${feature_value.split('&&')[0]}"
                    val = float(feature_value.split('&&')[1])
                    print(f"Removing tracks with {feature} - {op}:{val}")
                    valid = self.sdb.filter_tracks_by_audio_feature(self.run_record['eligible_tracks'], {feature:{op:val}})
                    bad_tracks.extend([x for x in self.run_record['eligible_tracks'] if x not in valid])
                    print(f"Cumulative Bad Tracks found {len(np.unique(bad_tracks))}")

                
            elif filter_name == "artist_filter":
                if filter_value == 'hard':
                    # Limits output to tracks that contain only storm artists
                    for track in tqdm(self.run_record['eligible_tracks']):

                        track_artists = set(self.sdb.get_track_artists(track))
                        if not track_artists.issubset(set(self.run_record['storm_artists'])):
                            bad_tracks.append(track)

                elif filter_value == 'soft':
                    # Removes tracks that contain known filtered out artists
                    # Other 'bad' artists could sneak in if not tracked by storm
                    for track in tqdm(self.run_record['eligible_tracks']):
                        track_artists = set(self.sdb.get_track_artists(track))
                        if not set(self.run_record['removed_artists']).isdisjoint(track_artists):
                            bad_tracks.append(track)

            else:
                print(f"{filter_name} not supported or misspelled. ")

        bad_tracks = np.unique(bad_tracks).tolist()
        print("Removing bad tracks . . .")
        self.run_record['storm_tracks'] = [x for x in self.run_record['eligible_tracks'] if x not in bad_tracks]
        self.run_record['removed_tracks'] = bad_tracks
        print(f"Starting Track Amount: {len(self.run_record['eligible_tracks'])}")
        print(f"Ending Track Amount: {len(self.run_record['storm_tracks'])}")

    def filter_rereleases(self, last_delivered_window=60):
        """
        Uses a track generated unique id (artists + title) and a start_date
        to remove tracks that are most likely just re-released.

        Notes:
            - Typically remixes contain "remix" in title, they will still be included
            - The time window allows for potential unique songs to be released later, especially
            in film music. An entire album is not likely to be excluded
            - There ia no guarantee an explicit or non-explicit version will be kept
        """

        # Convert storm tracks to their uids
        print(f"Starting Storm Track Amount: {len(self.run_record['storm_tracks'])}")
        storm_tracks = self.sdb.get_track_info(self.run_record['storm_tracks'], {"_id":1, 'name':1, 'artists':1})
        storm_track_df = pd.DataFrame(storm_tracks)
        storm_track_df['track_uids'] = storm_track_df.apply(lambda x: self.sdb.gen_unique_track_id(x['name'], x['artists']), axis=1)
        self.run_record['storm_tracks_uid'] = storm_track_df['track_uids'].unique()

        # Get a list of all the previous delivered storm tracks
        runs = self.sdb.get_runs_by_storm(self.name)
        window_date = (dt.datetime.now() - dt.timedelta(days=last_delivered_window)).strftime('%Y-%m-%d')

        if runs is not None:
            valid_runs = [x for x in runs if x['run_date'] > window_date]
            print(f"Tracks remaining after dedup: {len(storm_track_df.drop_duplicates('track_uids')._id.tolist())}")

            delivered_tracks = []
            for run in valid_runs:

                # Not every run generates unique keys, bug will be fixed later
                if 'storm_tracks_uid' in run.keys():
                    delivered_tracks.extend(run['storm_tracks_uid'])
                    
            delivered_tracks = np.unique(delivered_tracks).tolist()

            # Remove previously delivered tracks
            storm_track_df = storm_track_df[~storm_track_df.track_uids.isin(delivered_tracks)]

        # Save off the unique track ids (dedupped on their unique name)
        self.run_record['storm_tracks'] = storm_track_df.drop_duplicates('track_uids')._id.tolist()
        self.run_record['storm_tracks_uid'] = storm_track_df['track_uids'].unique().tolist()
        print(f"Ending Storm Track Amount: {len(self.run_record['storm_tracks'])}")