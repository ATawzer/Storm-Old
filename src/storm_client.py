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
                                                redirect_uri='http://localhost:8080/')
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

