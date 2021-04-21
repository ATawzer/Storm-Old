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
#print = slow_print # for fun
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
        q = {"release_date":{"$gte": start_date, "$lt": end_date}}
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
            




class StormClient:

    def __init__(self, user_id):

        self.scope = 'user-follow-read playlist-modify-private playlist-modify-public user-follow-modify' # scope for permissions
        self.user_id = user_id
        self.client_id = os.getenv('storm_client_id') # API app id
        self.client_secret = os.getenv('storm_client_secret') # API app secret

        # Spotify API connection
        self.sp_cc = oauth2.SpotifyClientCredentials(self.client_id, self.client_secret)
        self.token = None

        # Authenticate
        self.refresh_connection()

        # Good
        print("Storm Client successfully connected to Spotify.\n")


    # Authentication
    def refresh_connection(self):
        """
        Get a cached token (again) or try to get a new one. 
        Call this before any api call to make sure it won't get credential error.
        """
        try:
            self.token = self.sp_cc.get_access_token(as_dict=False)
            self.sp = spotipy.Spotify(auth=self.token)
        except:
            print("Looks like a new User, couldn't get access token. Trying authenticating.")
            self.token = util.prompt_for_user_token(self.user_id,
                                                        scope=self.scope,
                                                        client_id=self.client_id,
                                                        client_secret=self.client_secret,
                                                        redirect_uri='http://localhost/')
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
            result.extend([{k: x[k] for k in keys} for x in response])

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
        self.name = storm_name

        # metadata
        self.run_date = dt.datetime.now().strftime('%Y-%m-%d')
        self.run_record = {'config':self.config, 
                           'storm_name':self.name,
                           'run_date':self.run_date,
                           'playlists':[],
                           'input_tracks':[],
                           'input_artists':[]}
        self.last_run = self.sdb.get_last_run(self.name)

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

    # Low Level orchestration
    def load_playlist(self, playlist_id):
        """
        Pulls down playlist info and writes it back to db
        """

        # Determine if playlists need examining
        if self.run_date != self.sdb.get_playlist_collection_date(playlist_id):

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

    
        
class Storm:
    """
    Main callable that initiates and saves storm data
    """
    def __init__(self, storm_names, start_date=None):

        self.print_initial_screen()
        self.sdb = StormDB()
        self.storm_names = storm_names
        self.start_date = start_date
        self.runners = {}

    def print_initial_screen(self):

        print("A Storm is Brewing. . .\n")
        time.sleep(.5)
        
    def Run(self):

        print("Spinning up Storm Runners. . . ")
        for storm_name in self.storm_names:
            self.runners[storm_name] = StormRunner(storm_name)


    """
    Single object for running and saving data frm the storm run. Call Storm.Run() to generate a playlist from
    saved artists.
    """
    def __init__(self, user_id, inputs, output, archive, name, start_date=None, filter_unseen=True, instrumental=True):
        """
        params:
            user_id - spotify user account number
            inputs - Dictionary of playlists 'name':'playlist_id' that will feed new releases
            output - Playlist id to save new releases to
            archive - Playlist id to archive current songs in the storm to
            name - A name for this storm setup (for saving metadata and allowing for multiple storm configurations)
            start_date - defaults to a 2-day window frm current date, but could be wider if desired (format: 'yyyy-mm-dd')
        """
        # Variables
        self.scope = 'user-follow-read playlist-modify-private playlist-modify-public user-follow-modify' # scope for permissions
        self.user_id = user_id
        self.client_id = os.getenv('client_id') # API app id
        self.client_secret = os.getenv('client_secret') # API app secret
        self.token = None
        self.token_start = None
        self.sp = None
        self.inputs = inputs
        self.output = output
        self.archive = archive
        self.name = name
        self.start_date = start_date
        self.window_date = None
        self.filter_unseen = filter_unseen
        self.instrumental = instrumental
        
        # Initialization
        self.authenticate()
        self.gen_dates()
        
        # I/O Params for file saving
        self.artist_id_csv = './data/storm_artists_'+self.name+'.csv'
        self.album_id_csv = './data/storm_albums_'+self.name+'.csv'
        self.md_name = './data/storm_run_metadata_'+self.name+'.csv'
        
        # Dataframe initialization
        self.blacklist = []
        self.artist_ids = []
        self.album_ids = []
        self.albums = pd.DataFrame(columns = ['album_group', 'album_type', 'artists', 'available_markets',
                               'external_urls', 'href', 'id', 'images', 'name', 'release_date',
                               'release_date_precision', 'total_tracks', 'type', 'uri'])
        self.new_ablums = pd.DataFrame()
        self.new_tracks = pd.DataFrame(columns = ['artists', 'available_markets', 'disc_number', 'duration_ms',
                               'explicit', 'external_urls', 'href', 'id', 'is_local', 'name',
                               'preview_url', 'track_number', 'type', 'uri'])
        self.storm_track_ids = []
        
        
        # Metadata for post-run reports
        self.mdf = pd.read_csv(self.md_name).set_index('run_date')
        self.rd = dt.datetime.now().strftime("%Y/%m/%d")
        self.mdf.loc[self.rd, 'start_date'] = self.start_date
        
            
    # Authentication Functions
    def authenticate(self):
        """
        Connect to Spotify API, intialize spotipy object and generate access token.
        """
        print("Generating Token and Authenticating. . .")
        self.token = util.prompt_for_user_token(self.user_id,
                                                scope=self.scope,
                                                client_id=self.client_id,
                                                client_secret=self.client_secret,
                                                redirect_uri='http://localhost/')
        self.sp = spotipy.Spotify(auth=self.token)
        self.token_start = dt.datetime.now()
        print("Authentication Complete.")
        print()
    
    def check_token(self):
        """
        Determine if token is still valid. This is called in many methods to avoid timeout
        """
        
        if abs((self.token_start - dt.datetime.now()).total_seconds()) < 3580:
            return True
        else:
            print("Awaiting Expiration and Refreshing.")
            time.sleep(25)
            self.authenticate()

    def gen_dates(self):
        """
        Generates a window-date to filter album release dates based on start-date
        """
        
        # Start Dates
        if self.start_date == None:
            self.start_date = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
            
        # Playlist Cycling dates
        self.window_date = (dt.datetime.now() - dt.timedelta(days=14)).strftime("%Y-%m-%d")
     
    
    # Ochestration Function
    def Run(self):
        """
        The function that a user must run to generate their playlist of new releases.
        Call this function after building a storm object
        
        Example Usage:
        storm = Storm(params)
        storm.Run() # Use parameters to generate releases
        """
        # Read-in existing data from past runs
        self.read_in()
        
        # Augment artist list before track collection
        self.augment_artist_list()
        self.clean_artists()
        self.save_artists()
        
        # Get Album lists
        self.get_artist_albums()
        self.filter_albums()

        # Tracks
        self.get_album_tracks()
        self.clean_tracks()

        # if track list to large apply date filter
        if len(self.storm_track_ids)>9999:
            self.filter_unseen = True
            self.filter_albums()
            self.get_album_tracks()
            self.clean_tracks()
        
        # Playlist Writing
        self.archive_current()
        self.add_tracks_to_playlist(self.output, self.storm_track_ids)
        
        # Metadata save
        self.save_md()
        self.save_albums()
        
    
    # I/O
    # methods in this section are straightforward and mostly used for metadata
    # tracking and simplifying the number of API calls using information fr0m
    # past runs
    def read_in(self):
        """
        Storm init function to gather 
        """
        print("Reading in existing Data.")
        
        if path.exists(self.artist_id_csv):
            print("Storm Arists Found! Reading in now.")
            self.artist_ids = pd.read_csv(self.artist_id_csv)['artists'].values.tolist()
            self.mdf.loc[self.rd, 'artists_tracked'] = len(self.artist_ids)
            print(f"Done! {len(self.artist_ids)} Unique Artists found.")
            
        else:
            self.mdf.loc[self.rd, 'artists_tracked'] = 0
        print()
            
        if path.exists('storm_blacklist_'+self.name+'.csv'):
            print("Blacklisted Arists Found! Reading in now.")
            self.blacklist = pd.read_csv('storm_blacklist_'+self.name+'.csv')['artists'].tolist()
            self.mdf.loc[self.rd, 'blacklisted_artists'] = len(self.blacklist)
            print(f"Done! {len(self.blacklist)} Blacklisted Artists found.")
        print()
            
        if path.exists(self.album_id_csv):
            print("Previously Discovered Albums Found! Reading in now.")
            self.album_ids = pd.read_csv(self.album_id_csv)['albums'].values.tolist()
            self.mdf.loc[self.rd, 'albums_tracked'] = len(self.album_ids)
            print(f"Done! {len(self.album_ids)} Albums found.") 
            
        else:
            self.mdf.loc[self.rd, 'albums_tracked'] = 0
        print()
    
    def save_artists(self):
        
        print("Saving Artist Ids.")
        pd.DataFrame(self.artist_ids, columns=['artists']).to_csv(self.artist_id_csv, index=False)
    
    def save_albums(self):
        print("Saving Albums from run.")
        self.album_ids = self.albums.id.tolist()
        pd.DataFrame(self.album_ids, columns=['albums']).to_csv(self.album_id_csv, index=False)
    
    def save_md(self):
        
        print("Writing metadata from run.")
        self.mdf.to_csv(self.md_name)
    
    # Storm Aggregate Functions
    # These methods do the bulk of the API interfacing
    # Most functions take in the previous step and work with the API
    # to obtain all the data needed to progress the Run method forward
    def augment_artist_list(self):
        """
        Use playlist inputs to get a list of artists to track releases from
        output:
            Arists from playlists added to artist_ids
        """
        # Comb through playlists and get the artist ids
        print("Augmenting new Artists from playlist input dictionary.")
        for pl in self.inputs.keys():
            print("Obtaining a list of Tracks from Playlist . . ." + pl)
            playlist_df = self.get_playlist_tracks(self.inputs[pl])

            print("Finding Artists . . .")
            self.extend_artists(playlist_df['track'])
        
        print("Done! All Input Playlists Scanned.")

    def get_playlist_tracks(self, playlist_id):
        """
        Obtain all tracks from a playlist id
        input:
            playlist_id - input playlist that tracks will be collected for
        output:
            All tracks from playlist saved
        """
        lim = 50
        more_tracks = True
        offset=0

        self.check_token()
        playlist_results = self.sp.user_playlist_tracks(self.user_id, playlist_id, limit=lim, offset=offset)
        
        if len(playlist_results['items']) < lim:
                more_tracks = False

        while more_tracks:

            self.check_token()
            offset += lim
            batch = self.sp.user_playlist_tracks(self.user_id, playlist_id, limit=lim, offset=offset)
            playlist_results['items'].extend(batch['items'])

            if len(batch['items']) < lim:
                more_tracks = False

        response_df = pd.DataFrame(playlist_results['items'])
        return response_df
    
    def extend_artists(self, track_df):
        """
        Take a list of artists, get information and decide whether to include
        input:
            Dataframe of Tracks
        output:
            Cleaned set of artist ids to augment
        """
        for track in track_df:
            try:
                artists = dict(track)['artists']
            except:
                continue

            for artist in artists:
                if artist['id'] not in self.artist_ids:
                    self.check_token()
                    artist_info = self.sp.artist(artist['id'])
                    if 'classical' not in artist_info['genres']:
                        self.artist_ids.append(artist['id'])
    
    def clean_artists(self):
        """
        Remove any artists saved in the Storm's blacklist metadata file
        """
        print("Removing Blacklist Artists.")
        self.filter_blacklist()
    
    def clean_tracks(self):
        """
        Perform clean-up on list of newly released tracks
        """
        self.storm_track_ids = np.unique(self.storm_track_ids)
        self.new_tracks = self.new_tracks.drop_duplicates('id').reset_index(drop=True)
        newids = []
        
        print("Checking Tracks for bad features.")
        print("Starting track amount: "+str(len(self.new_tracks)))
        for index in tqdm(self.new_tracks.index):
            
            artists = self.new_tracks.loc[index, 'artists']
            check=True
            
            # Check artists
            for artist in artists:
                if artist['id'] in self.blacklist:
                     check = False
            
            # If still a valid track, check a few features
            if check:
                
                # Get track features
                af = self.sp.audio_features(self.new_tracks.loc[index, 'id'])[0]
                
                try:
                    if af['instrumentalness'] < .7:
                        check = False
                    elif af['speechiness'] > .32:
                        check = False
                    elif af['duration_ms'] < 60001:
                        check = False
                except:
                    continue
            
            # Remove if certain features don't clear
            if not self.instrumental:
                check = True

            if check:
                 newids.append(self.new_tracks.loc[index, 'id'])
        print("Ending Track Amount: " + str(len(newids)))
        self.storm_track_ids = newids
        self.mdf.loc[self.rd, 'tracks_added'] = len(self.storm_track_ids)
        self.mdf.loc[self.rd, 'tracks_removed'] = self.mdf.loc[self.rd, 'tracks_eligible'] - self.mdf.loc[self.rd, 'tracks_added']
    
    def filter_classical(self):
        """
        Classical music filters on artist
        """
        output_list = []
        for artist in tqdm(self.artist_ids):
            self.check_token()
            artist_info = self.sp.artist(artist)

            if 'classical' not in artist_info['genres']:
                output_list.append(artist)

        self.artist_ids = output_list
        
    def filter_blacklist(self):
        """
        Blacklist metadata file filter
        """
        output_list = []
        for artist in tqdm(self.artist_ids):
            if artist not in self.blacklist:
                output_list.append(artist)

        self.artist_ids = output_list
        self.mdf.loc[self.rd, 'artists_augmented'] = len(self.artist_ids)-self.mdf.loc[self.rd, 'artists_tracked']
    
    def get_artist_albums(self):
        """
        Get a list of all albums an artist has released
        """        
        
        print("Obtaining all albums from the list of artists. (Albums)")
        lim = 50
        for artist_id in tqdm(self.artist_ids):
            
            self.check_token()
            response = self.sp.artist_albums(artist_id, limit=lim, album_type='album', country='US')
            offset = 0
            more_albums = True

            while more_albums:
                
                self.check_token()
                batch = self.sp.artist_albums(artist_id, limit=lim, offset=offset, album_type='album', country='US')
                response['items'].extend(batch['items'])
                offset += lim

                if len(batch['items']) < lim:
                        more_albums = False

            response_df = pd.DataFrame(response['items'])
            self.albums = pd.concat([self.albums, response_df], axis=0)
           
        print(f"Albums being tracked: {len(self.albums)}")
        print("Obtaining all albums from the list of artists. (Singles)")
        for artist_id in tqdm(self.artist_ids):
            
            self.check_token()
            response = self.sp.artist_albums(artist_id, limit=lim, album_type='single', country='US')
            offset = 0
            more_albums = True

            while more_albums:
                
                self.check_token()
                batch = self.sp.artist_albums(artist_id, limit=lim, offset=offset, album_type='single', country='US')
                response['items'].extend(batch['items'])
                offset += lim

                if len(batch['items']) < lim:
                        more_albums = False

            response_df = pd.DataFrame(response['items'])
            response_df = response_df
            self.albums = pd.concat([self.albums, response_df], axis=0)
            
        print(f"Albums being tracked: {len(self.albums)}")
   
    def filter_albums(self):
        """
        If filter_unseen is True, only releases in the window are tracked. Otherwise
        any new piece will be added.
        """
        # Or Condition, either its new or hasn't been viewed
        print("Filtering Album list for new content.")
        if self.filter_unseen:
            self.new_albums = self.albums[self.albums.release_date >= self.start_date]
        else:
            self.new_albums = self.albums[(~self.albums.id.isin(self.album_ids)) | (self.albums.release_date >= self.start_date)]

        self.mdf.loc[self.rd, 'albums_augmented'] = len(self.new_albums)
          
    def get_album_tracks(self):
        """
        Get all tracks off an album.
        """
        lim = 50
        print("Using Filtered albums to obtain a track list.")
        for album_id in tqdm(self.new_albums.id):
            self.check_token()
            response = self.sp.album_tracks(album_id, limit=lim)
            offset = 0
            more_tracks = True
            if len(response['items']) < lim:
                    more_tracks = False

            while more_tracks:
                
                self.check_token()
                batch = self.sp.album_tracks(album_id, limit=lim, offset=offset)
                response['items'].extend(batch['items'])
                offset += lim

                if len(batch['items']) < lim:
                    more_tracks = False

            response_df = pd.DataFrame(response['items'])
            self.new_tracks = pd.concat([self.new_tracks, response_df], axis=0)
        self.mdf.loc[self.rd, 'tracks_eligible'] = len(self.new_tracks)
    
    def archive_current(self):
        """
        Stash files still in output playlist to new playlist
        """
        # Read-in current tracks
        print("Archiving Current Storm Listening.")
        current_listening = self.get_playlist_tracks(self.output)
        current_archive = self.get_playlist_tracks(self.archive)
        
        try:
            track_ids_cur = [dict(track)['id'] for track in current_listening.track]
            track_ids_arc = [dict(track)['id'] for track in current_archive.track]
            track_ids_writing = []

            for track in track_ids_cur:
                if track not in track_ids_arc:
                    track_ids_writing.append(track)

            # Write them to the archive playlist
            if len(track_ids_writing) == 0:
                print("No Unique tracks to Archive.")
            else:
                self.add_tracks_to_playlist(self.archive, track_ids_writing, replace=False)
        except:
            print("No Tracks to Archive.")
    
    def add_tracks_to_playlist(self, playlist_id, track_ids, replace=True):
        """
        Write new releases to output playlist.
        """
        print("Preparing Tracks for Writing")
        lim = 50
        if len(self.storm_track_ids) > lim:
            split_tracks = np.array_split(track_ids, np.ceil(len(track_ids)/lim))

            print("Writing Tracks")
            if replace:
                self.check_token()
                self.sp.user_playlist_replace_tracks(self.user_id, playlist_id, split_tracks[0])
                for track_list in tqdm(split_tracks[1:]):
                    self.check_token()
                    self.sp.user_playlist_add_tracks(self.user_id, playlist_id, track_list)
            else:
                for track_list in tqdm(split_tracks):
                    self.check_token()
                    self.sp.user_playlist_add_tracks(self.user_id, playlist_id, track_list)
        else:
            print("Writing Tracks")
            if replace:
                self.check_token()
                self.sp.user_playlist_replace_tracks(self.user_id, playlist_id, self.storm_track_ids)
            else:
                self.check_token()
                self.sp.user_playlist_add_tracks(self.user_id, playlist_id, self.storm_track_ids)