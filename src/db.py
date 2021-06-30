import os
from sys import getsizeof
import json
from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import numpy as np
import datetime as dt
from timeit import default_timer as timer
import pymysql
from sqlalchemy import create_engine


from dotenv import load_dotenv
load_dotenv()

class StormDB:
    """
    Manages the MongoDB connections, reading and writing.
    Eventually would be an API service for accessing the Storm database which
    is essentially storm metadata and a small subset of the spotify database
    needed for storm operations and machine learning.
    """
    def __init__(self):

        # Build mongo client and db
        self.mc = MongoClient(os.getenv('mongo_host'),
                             username=os.getenv('mongo_user'),
                             password=os.getenv('mongo_pass'),
                             authSource=os.getenv('mongo_db'),
                             authMechanism='SCRAM-SHA-256')
        self.db = self.mc[os.getenv('mongo_db')]

        # initialize collections
        self.artists = self.db['artists']
        self.albums = self.db['albums']
        self.storms = self.db['storm_metadata']
        self.tracks = self.db['tracks']
        self.utracks = self.db['tracks_unique']
        self.playlists = self.db['playlists']
        self.runs = self.db['runs']
        self.blacklists = self.db['blacklists']

    # Metadata Reading endpoints
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
        q = {"storm_name":storm_name}
        cols = {"_id":0}
        r = list(self.runs.find(q, cols))

        if len(r) == 0:
            return None
        elif len(r) > 0:
            max_run_idx = np.argmax(np.array([dt.datetime.strptime(x['run_date'], '%Y-%m-%d') for x in r]))
            return r[max_run_idx]

    def get_runs_by_storm(self, storm_name):
        """
        Will Return all run records for a storm (and all fields)
        """
        q = {"storm_name":storm_name}
        cols = {"config":0}
        r = list(self.runs.find(q, cols))

        if len(r) == 0:
            return None
        else:
            return r

    # Metadata Write Endpoints
    def write_run_record(self, run_record):

        q = {}
        self.runs.insert_one(run_record)

    # Playlist Reading Endpoints
    def get_playlists(self, name=False):
        """
        Returns all playlist ids in stormdb as a list, or as their names if you'd rather
        """
        q = {}
        cols = {"_id":1, "info":1}
        r = list(self.playlists.find(q, cols))

        if name:
            return [x["info"]["name"] for x in r]
        else:
            return [x["_id"] for x in r]

    def get_playlist_current_info(self, playlist_id):
        """
        Returns a playlists full record excluding changelog
        """
        q = {"_id":playlist_id}
        cols = {"changelog":0}
        r = list(self.playlists.find(q, cols))

        if len(r) == 0:
            raise Exception(f"{playlist_id} not found.")
        else:
            return r[0]

    def get_playlist_changelog(self, playlist_id):
        """
        Returns a playlists changelog, a dictionary where each entry is a date.
        """
        q = {"_id":playlist_id}
        cols = {"changelog":1}
        r = list(self.playlists.find(q, cols))

        if len(r) == 0:
            raise Exception(f"{playlist_id} not found.")
        else:
            if 'changelog' in r[0].keys():
                return r[0]['changelog']
            else:
                raise Exception(f"No changelog found for {playlist_id}, has it been collected more than once?")

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

    # Playlist Write Endpoints
    def update_playlist(self, playlist_record):

        q = {'_id':playlist_record['_id']}

        # Add new entry or update existing one
        record = playlist_record
        changelog_update =  {
                            'snapshot':playlist_record['info']['snapshot_id'],
                            'tracks':playlist_record['tracks']
                            }

        # Update static fields
        exclude_keys = ['changelog']
        update_dict = {k: playlist_record[k] for k in set(list(playlist_record.keys())) - set(exclude_keys)}
        self.playlists.update_one(q, {"$set":record}, upsert=True)

        # Push to append fields (date as new key)
        for key in exclude_keys:
            self.playlists.update_one(q, {"$set":{f"{key}.{playlist_record['last_collected']}":changelog_update}}, upsert=True)

    # Artist Reading Endpoints
    def get_known_artist_ids(self):
        """
        Returns all ids from the artists db.
        """

        q = {}
        cols = {"_id":1}
        r = list(self.artists.find(q, cols))

        return [x['_id'] for x in r]

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

    def get_artists_by_genres(self, genres):
        """
        Gets a list artists in DB that have one or more of the genres
        """
        q = {"genres":{"$all":genres}}
        cols = {"_id":1}
        r = list(self.artists.find(q, cols))

        return [x["_id"] for x in r]

    # Arist Write Endpoints
    def update_artists(self, artist_info_list):
        """
        Updates artist db with list of new artist info
        """

        for artist in tqdm(artist_info_list):
            q = {"_id":artist['id']}

            # Writing updates (formatting changes)
            artist['last_updated'] = dt.datetime.now().strftime('%Y-%m-%d')
            artist['total_followers'] = artist['followers']['total']
            del artist['followers']
            del artist['id']

            self.artists.update_one(q, {"$set":artist}, upsert=True)

    def update_artist_album_collected_date(self, artist_ids, date=None):
        """
        Updates a list of artists album_collected date to today by default.
        """
        date = dt.datetime.now().strftime('%Y-%m-%d') if date is None else date

        for artist_id in tqdm(artist_ids):
            q = {"_id":artist_id}
            self.artists.update_one(q, {"$set":{"album_last_collected":date}}, upsert=True)

    # Blacklist Read Enpoints
    def get_blacklist(self, name):
        """
        Returns a full blacklist record by name (id)
        """
        q = {"_id":name}
        cols = {"_id":1, "blacklist":1, "type":1, "input_playlist":1}
        return list(self.blacklists.find(q, cols))

    # Blacklist Write Endpoints
    def update_blacklist(self, blacklist_name, artists):
        """
        updates a blacklists artists given its name
        """
        q = {"_id":blacklist_name}
        [self.blacklists.update_one(q, {"$addToSet":{"blacklist":x}}) for x in artists]

    # Album Read Endpoints
    def get_albums_by_release_date(self, start_date, end_date):
        """
        Get all albums in date window
        """
        q = {"release_date":{"$gt": start_date, "$lte": end_date}}
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
        q = {"_id":{"$in":valid_albums}, "release_date":{"$gt": start_date, "$lte": end_date}}
        cols = {"_id":1}
        r = list(self.albums.find(q, cols))

        return [x['_id'] for x in r]

    def get_album_info(self, album_ids, fields={"added_to_artists":0, "tracks":0, "artists":0}):
        """
        Returns specified information about a list of albums.
        """

        id_lim = 50000
        batches = np.array_split(album_ids, int(np.ceil(len(album_ids)/id_lim)))
        result = []
        for batch in tqdm(batches):

            q = {"_id":{"$in":batch.tolist()}}
            cols = fields
            r = list(self.albums.find(q, cols))
            result.extend(r)

        return result

    # Album Write Endpoints
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

    # Track Read Endpoints
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

    def get_tracks_from_albums(self, albums):
        """
        returns a track list based on an album list
        """
        q = {"album_id":{"$in":albums}}
        cols = {"_id":1}
        r = list(self.tracks.find(q, cols))

        return [x["_id"] for x in r]

    def get_track_artists(self, track):

        q = {"_id":track}
        cols = {"_id":1, "artists":1}

        try:
            return list(self.tracks.find(q, cols))[0]['artists']
        except:
            return [] # not good, for downstream bug fixing
            raise ValueError(f"Track {track} not found or doesn't have any artists.")

    def get_tracks(self):
        """
        Returns a list of all tracks in the database.
        """
        q = {}
        cols = {"_id":1}
        r = list(self.tracks.find(q, cols))

        return [x["_id"] for x in r]

    def get_track_info(self, track_ids, fields={"artists":0, "audio_analysis":0}):
        """
        Returns all available information for every track in track_ids.
        Done in batches as it is a large database.
        """

        # Check if needs to be done in batches
        id_lim = 50000
        batches = np.array_split(track_ids, int(np.ceil(len(track_ids)/id_lim)))
        result = []
        for batch in tqdm(batches):

            q = {"_id":{"$in":batch.tolist()}}
            cols = fields
            r = list(self.tracks.find(q, cols))
            result.extend(r)

        return result


    # Track Write Endpoints
    def update_tracks(self, track_info_list):
        """
        Updates a track and its album frm a list.
        """

        for track in tqdm(track_info_list):

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

    # DB Cleanup and Prep / other
    def filter_tracks_by_audio_feature(self, tracks, audio_filter):
        """
        Takes in a specific audio_filter format to get tracks with a filter
        """
        q = {"_id":{"$in":tracks}, **audio_filter}
        cols = {"_id":1}
        r = list(self.tracks.find(q, cols))

        return [x["_id"] for x in r]

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

    def gen_unique_track_id(self, track_name, artists):
        """
        Consistent way to clean track names for database writing and id structure
        """

        bad_chars = ',. '
        for char in bad_chars:
            track_name = track_name.replace(char, '')
        artist_string = "A&A".join(artists)
        return track_name+"T&A"+artist_string

    def dedup_tracks_on_name(self, updated_date='2021-01-01'):
        """
        Copies the track database to the tracks_unique database.
        This database is dedicated to unique songs based on artist and name.
        """

        # Get tracks that need moving
        q = {"last_updated":{"$gte":updated_date}}
        cols = {"_id":1}
        r = list(self.tracks.find(q, cols))

        # Move them in batches
        batch_size = 100
        batches = np.array_split(r, int(np.ceil(len(r)/batch_size)))

        for batch in tqdm(batches):

            q = {"_id":{"$in":[x["_id"] for x in batch.tolist()]}}
            cols = {"_id":0, "last_updated":0}
            r = list(self.tracks.find(q, cols))

            # Add track data to tracks
            for track_record in r:
                track_record['_id'] = self.gen_unique_track_id(track_record["name"], track_record['artists'])

                q = {"_id":track_record["_id"]}
                track_record['dedup_date'] = dt.datetime.now().strftime('%Y-%m-%d')
                self.utracks.update_one(q, {"$set":track_record}, upsert=True)

    #def gen_run_history(self)



class StormAnalyticsDB:
    """
    Wrapper for the MySQL analytics database. Unlike StormDB, 
    this is a basic anlaytics database and thus does not require
    much schema abstraction code, as most decisions are built into
    the view generator themselves. This just manages the IO of those
    views so that connection to the database is managed in a single object.
    """

    def __init__(self):

        cxn_string = "mysql+pymysql://{user}:{password}@{host}/{database}?host={host}?port={port}"
        self.db_engine = create_engine(cxn_string.format(
                                user=os.getenv("mysql_user"),
                                password=os.getenv("mysql_pass"),
                                host=os.getenv("mysql_server"),
                                database=os.getenv("mysql_db"),
                                port=os.getenv("mysql_port")))
        self.cxn = self.db_engine.connect()

    def read_table(self, table=None, q=None):
        """
        Reads a table from the SADB by name or query
        """

        if q is None:
            df = pd.read_sql_table(table, self.cxn)
        else:
            df = pd.read_sql_query(q, self.cxn)

        return df

    def write_table(self, table, df, method='overwrite', schema='storm_analytics'):
        """
        writes a pandas dataframe into the DB.
        """

        if method == "overwrite":
            df.to_sql(table, self.cxn, if_exists='replace', schema=schema, index=False)