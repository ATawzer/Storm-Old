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
from pymongo import MongoClient
from dotenv import load_dotenv

# Internal
from helper import *
print = slow_print # for fun
load_dotenv()

class StormDB:
    """
    Manages the MongoDB connections, reading and writing.
    """
    def __init__(self):

        # Build mongo client and db
        self.mc = MongoClient(os.getenv('mongo_uri'))
        self.db = self.mc[os.getenv('db_name')]

        # initialize collections
        self.artists = self.db['artists']
        self.albums = self.db['albums']
        self.storms = self.db['storm_metadata']
        self.tracks = self.db['tracks']
        self.playlists = self.db['playlists']
        self.runs = self.db['runs']
        self.blacklists = self.db['blacklists']

    def get_config(self, storm_name):
        """
        returns a storm configuration given its name, assuming it exists.
        """
        q = {'name':storm_name}
        cols = {'config':1}
        r = list(self.storms.find(q, cols))

        if len(r) == 0:
            raise KeyError(f"{storm_name} not found, no configuration to load.")
        else:
            return r[0]['config']

    def get_all_configs(self):
        """
        Returns all configurations in DB.
        """
        q = {}
        cols = {"name":1, "_id":0}
        r = list(self.storms.find(q, cols))

        return [x['name'] for x in r]

    def get_last_run(self, storm_name):
        """
        returns the run_record from last storm run under a given name
        """
        q = {"name":storm_name}
        cols = {}
        r = list(self.runs.find(q, cols))

        if len(r) == 0:
            return None
        elif len(r) > 0:
            max_run_idx = np.argmax(np.array([dt.datetime(x['run_date']) for x in r]))
            return r[max_run_idx]

    def write_run_record(self, run_record):

        q = {}
        self.runs.insert_one(run_record)

    # Playlist
    def get_playlist_collection_date(self, playlist_id):
        """
        Gets a playlists last collection date.
        """
        q = {"_id":playlist_id}
        cols = {"last_collected":1}
        r = list(self.playlists.find(q, cols))

        # If not found print old date
        if len(r) == 0:
            return '2000-01-01' # Long ago
        elif len(r) == 1:
            return r[0]['last_collected']
        else:
            raise Exception("Playlist Ambiguous, should be unique to table.")

    def update_playlist(self, pr):

        q = {'_id':pr['_id']}

        # Add new entry or update existing one
        record = pr
        changelog_update =  {
                            'snapshot':pr['info']['snapshot_id'],
                            'tracks':pr['tracks']
                            }

        # Update static fields
        exclude_keys = ['changelog']
        update_dict = {k: pr[k] for k in set(list(pr.keys())) - set(exclude_keys)}
        self.playlists.update_one(q, {"$set":record}, upsert=True)

        # Push to append fields (date as new key)
        for key in exclude_keys:
            self.playlists.update_one(q, {"$set":{f"{key}.{pr['last_collected']}":changelog_update}}, upsert=True)

    def get_loaded_playlist_tracks(self, playlist_id):
        """
        Returns a playlists most recently collected tracks
        """
        q = {"_id":playlist_id}
        cols = {'tracks':1, "_id":0}
        r = list(self.playlists.find(q, cols))
        
        if len(r) == 0:
            raise ValueError(f"Playlist {playlist_id} not found.")
        else:
            return r[0]['tracks']

    def get_loaded_playlist_artists(self, playlist_id):
        """
        Returns a playlists most recently collected artists
        """
        q = {"_id":playlist_id}
        cols = {'artists':1, "_id":0}
        r = list(self.playlists.find(q, cols))
        
        if len(r) == 0:
            raise ValueError(f"Playlist {playlist_id} not found.")
        else:
            return r[0]['artists']

    # Artists
    def get_known_artist_ids(self):
        """
        Returns all ids from the artists db.
        """

        q = {}
        cols = {"_id":1}
        r = list(self.artists.find(q, cols))

        return [x['_id'] for x in r]

    def update_artists(self, artist_info):
        """
        Updates the artist db with new info
        """

        for artist in tqdm(artist_info):
            q = {"_id":artist['id']}

            # Writing updates (formatting changes)
            artist['last_updated'] = dt.datetime.now().strftime('%Y-%m-%d')
            artist['total_followers'] = artist['followers']['total']
            del artist['followers']
            del artist['id']

            self.artists.update_one(q, {"$set":artist}, upsert=True)

    def get_artists_for_album_collection(self, max_date):
        """
        returns all artists with album collection dates before max_date.
        """
        q = {}
        cols = {"_id":1, "album_last_collected":1}
        r = list(self.artists.find(q, cols))

        # Only append artists who need collection in result
        result = []
        for artist in r:
            if 'album_last_collected' in artist.keys():
                if artist['album_last_collected'] < max_date:
                    result.append(artist['_id'])
            else:
                result.append(artist['_id'])
        return result

    def update_artist_album_collected_date(self, artist_ids):
        """
        Updates a list of artists album_collected date to today.
        """
        date = dt.datetime.now().strftime('%Y-%m-%d')

        for artist_id in tqdm(artist_ids):
            q = {"_id":artist_id}
            self.artists.update_one(q, {"$set":{"album_last_collected":date}}, upsert=True)

    def get_blacklist(self, name):
        """
        Returns a full blacklist record by name (id)
        """
        q = {"_id":name}
        cols = {"_id":1, "blacklist":1, "type":1, "input_playlist":1}
        return list(self.blacklists.find(q, cols))

    def get_artists_by_genres(self, genres):
        """
        Gets a list artists in DB that have one or more of the genres
        """
        q = {"genres":{"$all":genres}}
        cols = {"_id":1}
        r = list(self.artists.find(q, cols))

        return [x["_id"] for x in r]

    def update_blacklist(self, blacklist_name, artists):
        """
        updates a blacklists artists given its name
        """
        q = {"_id":blacklist_name}
        [self.blacklists.update_one(q, {"$addToSet":{"blacklist":x}}) for x in artists]

    # Albums
    def update_albums(self, album_info):
        """
        update album info if needed.
        """

        for album in tqdm(album_info):
            q = {"_id":album['id']}

            # Writing updates (formatting changes)
            album['last_updated'] = dt.datetime.now().strftime('%Y-%m-%d')
            del album['id']

            self.albums.update_one(q, {"$set":album}, upsert=True)

    def get_albums_by_release_date(self, start_date, end_date):
        """
        Get all albums in date window
        """
        q = {"release_date":{"$gte": start_date, "$lte": end_date}}
        cols = {"_id":1}
        r = list(sdb.albums.find(q, cols))

        return [x['_id'] for x in r]

    def get_albums_for_track_collection(self):
        """
        Get all albums that need tracks added.
        """
        q = {}
        cols = {"_id":1, "tracks":1}
        r = list(self.albums.find(q, cols))

        # Only append artists who need collection in result
        result = []
        for album in r:
            if 'tracks' not in album.keys():
                result.append(album['_id'])
        return result
    
    def get_albums_from_artists_by_date(self, artists, start_date, end_date):
        """
        Get all albums in date window
        """

        # Get starting list of albums with artists
        q = {"_id":{"$in":artists}}
        cols = {"albums":1}
        r = list(self.artists.find(q, cols))

        valid_albums = []
        [valid_albums.extend(x['albums']) for x in r if 'albums' in x]

        # Return the albums in this list that also meet date criteria
        q = {"_id":{"$in":valid_albums}, "release_date":{"$gte": start_date, "$lte": end_date}}
        cols = {"_id":1}
        r = list(self.albums.find(q, cols))

        return [x['_id'] for x in r]

    # Tracks
    def update_tracks(self, track_info):
        """
        update track and its album info if needed.
        """

        for track in tqdm(track_info):

            # Add track to album record
            q = {'_id':track['album_id']}
            self.albums.update_one(q, {"$push":{"tracks":track['id']}}, upsert=True)

            # Add track data to tracks
            q = {"_id":track['id']}
            track['last_updated'] = dt.datetime.now().strftime('%Y-%m-%d')
            del track['id']
            self.tracks.update_one(q, {"$set":track}, upsert=True)

    def update_track_features(self, tracks):
        """
        Updates a track's record with audio features
        """
        for track in tqdm(tracks):
            q = {"_id":track['id']}

            # Writing updates (formatting changes)
            track['audio_features'] = True
            track['last_updated'] = dt.datetime.now().strftime('%Y-%m-%d')
            del track['id']

            self.tracks.update_one(q, {"$set":track}, upsert=True)

    def get_tracks_for_feature_collection(self):
        """
        Get all tracks that need audio features added.
        """
        q = {}
        cols = {"_id":1, "audio_features":1}
        r = list(self.tracks.find(q, cols))

        # Only append artists who need collection in result
        result = []
        for track in r:
            if 'audio_features' not in track.keys():
                result.append(track['_id'])
            else:
                if not track['audio_features']:
                    result.append(track['_id'])
        return result

    def update_bad_track_features(self, bad_tracks):
        """
        If tracks that can't get features are identified, mark them here
        """
        for track in tqdm(bad_tracks):
            q = {"_id":track['id']}

            # Writing updates (formatting changes)
            track['audio_features'] = False
            track['last_updated'] = dt.datetime.now().strftime('%Y-%m-%d')
            del track['id']

            self.tracks.update_one(q, {"$set":track}, upsert=True)

    def get_tracks_from_albums(self, albums):
        """
        returns a track list based on an album list
        """
        q = {"album_id":{"$in":albums}}
        cols = {"_id":1}
        r = list(self.tracks.find(q, cols))

        return [x["_id"] for x in r]

    def filter_tracks_by_audio_feature(self, tracks, audio_filter):
        """
        Takes in a specific audio_filter format to get tracks with a filter
        """
        q = {"_id":{"$in":tracks}, **audio_filter}
        cols = {"_id":1}
        r = list(self.tracks.find(q, cols))

        return [x["_id"] for x in r]

    def get_track_artists(self, track):

        q = {"_id":track}
        cols = {"_id":1, "artists":1}

        try:
            return list(self.tracks.find(q, cols))[0]['artists']
        except:
            raise ValueError(f"Track {track} not found or doesn't have any artists.")

    # DB Cleanup and Prep
    def update_artist_albums(self):
        """
        Adds a track list to each artist or appends if not there
        """

        q = {}
        cols = {"_id":1, "added_to_artists":1, 'artists':1}
        r = list(self.albums.find(q, cols))

        for album in tqdm(r):

            if 'added_to_artists' not in album.keys():
                for artist in album['artists']:
                    self.artists.update_one({"_id":artist}, {"$addToSet":{"albums":album["_id"]}}, upsert=True)
                self.albums.update_one({"_id":album["_id"]}, {"$set":{"added_to_artists":True}})
            else:
                if not album['added_to_artists']:
                    for artist in album['artists']:
                        self.artists.update_one({"_id":artist}, {"$addToSet":{"albums":album["_id"]}}, upsert=True)
                    self.albums.update_one({"_id":album["_id"]}, {"$set":{"added_to_artists":True}})

class StormUserClient:

    def __init__(self, user_id):
        """
        Client with authorization for modifying user information.
        """

        self.user_id = user_id # User to authorize, only needed for modify operations
        self.scope = 'playlist-modify-private playlist-modify-public' # scope for permissions
        self.client_id = os.getenv('storm_client_id') # API app id
        self.client_secret = os.getenv('storm_client_secret') # API app secret

        self.token = None

        # Authenticate
        self.authenticate()
        print("Storm User Client successfully connected to Spotify.")

    # Authentication Functions
    def authenticate(self):
        """
        Connect to Spotify API, intialize spotipy object and generate access token.
        """
        self.token = util.prompt_for_user_token(self.user_id,
                                                scope=self.scope,
                                                client_id=self.client_id,
                                                client_secret=self.client_secret,
                                                redirect_uri='http://localhost/')
        self.sp = spotipy.Spotify(auth=self.token)
        self.token_start = dt.datetime.now()

    def write_playlist_tracks(self, playlist_id, tracks):
        """
        Writes a list of track ids into a user's playlist
        """

        # Call info
        id_lim = 50
        batches = np.array_split(tracks, int(np.ceil(len(tracks)/id_lim)))

        # First batch overwrite
        self.authenticate()
        self.sp.user_playlist_replace_tracks(self.user_id, playlist_id, batches[0])

        for batch in tqdm(batches[1:]):
            self.sp.user_playlist_add_tracks(self.user_id, playlist_id, batch)

        return True

class StormClient:

    def __init__(self, user_id):
        """
        Simple client, no user needed
        """

        self.user_id = user_id # User scope, no authorization needed, though
        self.client_id = os.getenv('storm_client_id') # API app id
        self.client_secret = os.getenv('storm_client_secret') # API app secret

        # Spotify API connection
        self.sp_cc = oauth2.SpotifyClientCredentials(self.client_id, self.client_secret)
        self.token = None

        # Authenticate
        self.refresh_connection()

        # Good
        print("Storm Client successfully connected to Spotify.")


    # Authentication
    def refresh_connection(self):
        """
        Get a cached token (again) or try to get a new one. 
        Call this before any api call to make sure it won't get credential error.
        """
        self.token = self.sp_cc.get_access_token(as_dict=False)
        self.sp = spotipy.Spotify(auth=self.token)

    def get_playlist_info(self, playlist_id):
        """ Returns subset of playlist metadata """

        # params
        fields = 'description,id,name,owner,snapshot_id'

        # Get the info
        self.refresh_connection()
        return self.sp.playlist(playlist_id, fields=fields)

    def get_playlist_tracks(self, playlist_id):
        """
        Return subset of information about a playlists tracks (unique)
        """

        # Call info
        lim = 100
        offset = 0
        fields = 'items(track(id))' # only getting the ids, get info about them later
        
        # Get number of tracks trying to get (faster to know then go in blind)
        self.refresh_connection()
        total = int(self.sp.user_playlist_tracks(self.user_id, playlist_id, fields='total')['total'])
        print(f"Total Tracks: {total}")
        
        # loop through and append track ids
        result = ['' for x in range(total)] # List of track ids pre-initialized
        for i in tqdm(range(int(np.ceil(total/lim)))):
            self.refresh_connection()
            response = self.sp.user_playlist_tracks(self.user_id, playlist_id, fields=fields, limit=lim, offset=(i*lim))

            result[i*lim:(i*lim)+len(response['items'])] = [x['track']['id'] for x in response['items']]

        return np.unique(result).tolist()

    def get_artists_from_tracks(self, tracks):
        """
        Returns list of artist_ids given track_ids
        """

        # Call Info
        id_lim = 50
        batches = np.array_split(tracks, int(np.ceil(len(tracks)/id_lim)))

        # Get Artists
        artists = []
        for batch in tqdm(batches):
            self.refresh_connection()
            response = self.sp.tracks(batch, market='US')['tracks']
            [artists.extend(x['artists']) for x in response]

        # Filter to just ids
        return np.unique([x['id'] for x in artists]).tolist()

    def get_artist_info(self, artists):
        """
        Gets a subset of artist info from a list of ids
        """

        # Call info
        id_lim = 50
        keys = ['followers', 'genres', 'id', 'name', 'popularity']
        batches = np.array_split(artists, int(np.ceil(len(artists)/id_lim)))

        # Get All artist info
        result = []
        for batch in tqdm(batches):
            self.refresh_connection()
            response = self.sp.artists(batch)['artists']
            result.extend(response)

        # Filter to just relevant fields
        for i in range(len(result)):
            result[i] = {k: result[i][k] for k in keys}

        return result

    def get_artist_albums(self, artists):
        """
        Returns subset of album fields
        """

        # Call info
        lim = 50
        offset = 0
        album_types = 'single,album'
        country='US'
        keys = ['album_type', 'album_group', 'id', 'name', 'release_date', "artists", 'total_tracks']

        # Get All artist info
        result = []
        for artist in tqdm(artists):

            # Initialize array for speed
            self.refresh_connection()
            total = int(self.sp.artist_albums(artist, country=country, album_type=album_types, limit=1)['total'])

            artist_result = ['' for x in range(total)] # List of album ids pre-initialized
            for i in range(int(np.ceil(total/lim))):
                self.refresh_connection()
                response = self.sp.artist_albums(artist, country=country, album_type=album_types, limit=lim, offset=(i*lim))
                artist_result[i*lim:(i*lim)+len(response['items'])] = [{k: x[k] for k in keys} for x in response['items']]

            result.extend(artist_result)

        # Remove all other info about artists except ids
        for i in range(len(result)):
            result[i]['artists'] = [x['id'] for x in result[i]['artists']]

        return result

    def get_album_tracks(self, albums):
        """
        Returns an albums info and tracks.
        """
        # Call info
        lim = 50
        country = 'US'
        keys = ['artists', 'duration_ms', 'id', 'name', 'explicit', 'track_number']

        # Get All album tracks info
        result = []
        for album in tqdm(albums):

            # Initialize array for speed
            self.refresh_connection()
            total = int(self.sp.album_tracks(album, market=country, limit=1)['total'])

            album_result = ['' for x in range(total)] # List of album ids pre-initialized
            for i in range(int(np.ceil(total/lim))):
                self.refresh_connection()
                response = self.sp.album_tracks(album, market=country, limit=lim, offset=(i*lim))               
                album_result[i*lim:(i*lim)+len(response['items'])] = [{k: x[k] for k in keys} for x in response['items']]

            # Add the album_id back in
            [x.update({'album_id':album}) for x in album_result]
            result.extend(album_result)

        # Remove all other info about artists except ids
        for i in range(len(result)):
            result[i]['artists'] = [x['id'] for x in result[i]['artists']]

        return result

    def get_track_features(self, tracks):
        """
        Returns a tracks info and audio features
        """
        # Call info
        id_lim = 50
        keys = ["id", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness",
        "instrumentalness", "liveness", "valence", "tempo", "time_signature"]
        batches = np.array_split(tracks, int(np.ceil(len(tracks)/id_lim)))

        # Get track features in batches
        result = []
        for batch in tqdm(batches):
            self.refresh_connection()
            response = self.sp.audio_features(batch)
            result.extend([{k: x[k] for k in keys} for x in response if x is not None])

        # Filter to just ids
        return result

class StormRunner:
    """
    Orchestrates a storm run
    """
    def __init__(self, storm_name, start_date=None):

        print(f"Initializing Runner for {storm_name}")
        self.sdb = StormDB()
        self.config = self.sdb.get_config(storm_name)
        self.sc = StormClient(self.config['user_id'])
        self.suc = StormUserClient(self.config['user_id'])
        self.name = storm_name
        self.start_date = start_date

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
            self.run_record['input_artists'].extend(self.last_run['input_artists'])

    def collect_playlist_info(self):
        """
        Initial Playlist setup orchestration
        """

        print("Loading Great Targets . . .")
        self.load_playlist(self.config['great_targets'])

        print("Loading Good Targets . . .")
        self.load_playlist(self.config['great_targets'])

        # Check for additional playlists
        if 'additional_input_playlists' in self.config.keys():
            if self.config['additional_input_playlists']['is_active']:
                for ap, ap_id in self.config['additional_input_playlists']['playlists'].items():
                    print(f"Loading Additional Playlist: {ap}")
                    self.load_playlist(ap_id)
        
         ## ---- Future Version ----
        # Check if we need to move rolling
        
        # Check what songs remain in sample and full delivery
       
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

        print("Storm Tracks Generated! \n")

    def call_weatherboy(self):
        """
        Run Modeling process
        """
        return None

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
                self.start_date = self.last_run['run_date']
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

class Storm:
    """
    Main callable that initiates and saves storm data
    """
    def __init__(self, storm_names, start_date=None):

        self.print_initial_screen()
        self.sdb = StormDB()
        self.storm_names = storm_names
        self.runners = {}

    def print_initial_screen(self):

        print("A Storm is Brewing. . .\n")
        time.sleep(.5)
        
    def Run(self):

        print("Spinning up Storm Runners. . . ")
        for storm_name in self.storm_names:
            StormRunner(storm_name).Run()

Storm(['film_vg_instrumental', 'contemporary_lyrical'])
