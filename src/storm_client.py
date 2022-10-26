from dataclasses import dataclass
import spotipy
from spotipy import util
from spotipy import oauth2
import numpy as np
import logging
from tqdm import tqdm
import os
import datetime as dt

from typing import List, Dict

l = logging.getLogger("storm")

@dataclass
class StormUserClient:
    """
    The User Client handles communicating with User specific assets, such as playlists
    This must be authenticated and allowed by the user, specified at initialization with user_id
    """
    user_id: int
    scope: str = "playlist-modify-private playlist-modify-public"
    client_id: str = os.getenv("storm_client_id")  # API app id
    client_secret: str = os.getenv("storm_client_secret")  # API app secret
    token: str = None

    def __post_init__(self):
        """
        Client with authorization for modifying user information.
        """
        self._authenticate()
        l.debug("Storm User Client successfully connected to Spotify.")

    # Authentication Functions
    def _authenticate(self) -> None:
        """
        Connect to Spotify API, intialize spotipy object and generate access token.
        """
        self.token = util.prompt_for_user_token(
            self.user_id,
            scope=self.scope,
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri="http://localhost/",
        )
        self.sp = spotipy.Spotify(auth=self.token)
        self.token_start = dt.datetime.now()

    def write_playlist_tracks(self, playlist_id: int, tracks: List) -> None:
        """
        Writes a list of track ids into a user's playlist
        """

        # Call info
        id_lim = 50
        batches = np.array_split(tracks, int(np.ceil(len(tracks) / id_lim)))

        # First batch overwrite
        self._authenticate()
        self.sp.user_playlist_replace_tracks(self.user_id, playlist_id, batches[0])

        for batch in tqdm(batches[1:]):
            self.sp.user_playlist_add_tracks(self.user_id, playlist_id, batch)

        l.debug(f"Successfully Wrote {len(tracks)} Tracks to {playlist_id}")
        

@dataclass
class StormClient:
    """
    Unlike the User variant, the storm client provides functionality without needing to authenticate
    on the end users account. This is a High-level Storm API that wraps around the spotipy package
    which handles low-level API calls back to Spotify.
    """
    user_id: int
    client_id: str = os.getenv("storm_client_id")  # API app id
    client_secret: str = os.getenv("storm_client_secret")  # API app secret

    token: str = None

    def __post_init__(self):
        """
        Specify a user only for scope
        """

        self.sp_cc = oauth2.SpotifyClientCredentials(self.client_id, self.client_secret)

        # Authenticate
        self._authenticate()

        # Good
        l.debug("Storm Client successfully connected to Spotify.")

    # Authentication
    def _authenticate(self) -> None:
        """
        Get a cached token (again) or try to get a new one.
        Call this before any api call to make sure it won't get credential error.
        """
        self.token = self.sp_cc.get_access_token(as_dict=False)
        self.sp = spotipy.Spotify(auth=self.token)

    def get_playlist_info(self, playlist_id: int) -> Dict:
        """Returns subset of playlist metadata"""

        # params
        fields = "description,id,name,owner,snapshot_id"

        # Get the info
        self._authenticate()
        return self.sp.playlist(playlist_id, fields=fields)

    def get_playlist_tracks(self, playlist_id: int) -> List:
        """
        Return subset of information about a playlists tracks (unique)
        """

        # Call info
        batch_size = 100
        fields = "items(track(id))"  # only getting the ids, get info about them later

        # Get number of tracks trying to get (faster to know then go in blind)
        self._authenticate()
        total = int(self.sp.user_playlist_tracks(self.user_id, playlist_id, fields="total")["total"])
        num_batches = int(np.ceil(total / batch_size))

        l.debug(f"Total Tracks found for playlist {playlist_id}: {total}. Getting Tracks now . . .")

        # loop through and populate track ids
        result = ["" for x in range(total)]  # List of track ids pre-initialized

        for i in range(num_batches):
            self._authenticate()
            response = self.sp.user_playlist_tracks(
                self.user_id, playlist_id, fields=fields, limit=batch_size, offset=(i * batch_size)
            )

            # Populate Ids in the correct indices
            result[i * batch_size : (i * batch_size) + len(response["items"])] = [
                x["track"]["id"] for x in response["items"] if x["track"] is not None
            ]

        return np.unique(result).tolist()

    def get_artists_from_tracks(self, tracks: List) -> List:
        """
        Returns list of artist_ids given track_ids
        Return list will not be split by track, a list of unique artists
        across all of the tracks inputted.
        """

        # Call Info
        id_lim = 50
        batches = np.array_split(tracks, int(np.ceil(len(tracks) / id_lim)))

        l.debug(f"Getting Unique Artists for {len(tracks)} Tracks . . .")

        # Get Artists
        artists = []
        for batch in batches:
            self._authenticate()
            response = self.sp.tracks(batch, market="US")["tracks"]

            # Extend the artists array with all of the artists on the tracks
            [artists.extend(x["artists"]) for x in response]

        # Filter to just ids and unique artists
        return np.unique([x["id"] for x in artists]).tolist()

    def get_artist_info(self, artists: List) -> Dict:
        """
        Gets a subset of artist info from a list of ids
        """

        # Call info
        id_lim = 50
        keys = ["followers", "genres", "id", "name", "popularity"]
        batches = np.array_split(artists, int(np.ceil(len(artists) / id_lim)))

        l.debug(f"Getting {len(keys)} fields for {len(artists)} Artists . . .")

        # Get All artist info
        result = []
        for batch in batches:
            self._authenticate()
            response = self.sp.artists(batch)["artists"]
            result.extend(response)

        # Filter to just relevant fields
        for i in range(len(result)):
            result[i] = {k: result[i][k] for k in keys}

        return result

    def get_artist_albums(self, artists: List) -> Dict:
        """
        Returns subset of album fields
        """

        # Call info
        lim = 50
        offset = 0
        album_types = "single,album"
        country = "US"
        keys = [
            "album_type",
            "album_group",
            "id",
            "name",
            "release_date",
            "artists",
            "total_tracks",
        ]

        # Get All artist info
        result = []
        for artist in artists:

            # Initialize array for speed
            self._authenticate()
            total_albums = int(
                self.sp.artist_albums(
                    artist, country=country, album_type=album_types, limit=1
                )["total"]
            )

            artist_result = ["" for x in range(total_albums)]  # List of album ids pre-initialized

            for i in range(int(np.ceil(total_albums / lim))):
                self._authenticate()
                response = self.sp.artist_albums(
                    artist,
                    country=country,
                    album_type=album_types,
                    limit=lim,
                    offset=(i * lim),
                )
                artist_result[i * lim : (i * lim) + len(response["items"])] = [
                    {k: x[k] for k in keys} for x in response["items"]
                ]

            result.extend(artist_result)

        # Remove all other info about artists except ids
        for i in range(len(result)):
            result[i]["artists"] = [x["id"] for x in result[i]["artists"]]

        return result

    def get_album_tracks(self, albums: List) -> Dict:
        """
        Returns an albums info and tracks.
        """
        # Call info
        lim = 50
        country = "US"
        keys = ["artists", "duration_ms", "id", "name", "explicit", "track_number"]

        # Get All album tracks info
        result = []
        for album in albums:

            # Initialize array for speed
            self._authenticate()
            total = int(self.sp.album_tracks(album, market=country, limit=1)["total"])

            album_result = ["" for x in range(total)]  # List of album ids pre-initialized

            for i in range(int(np.ceil(total / lim))):
                self._authenticate()
                response = self.sp.album_tracks(
                    album, market=country, limit=lim, offset=(i * lim)
                )
                album_result[i * lim : (i * lim) + len(response["items"])] = [
                    {k: x[k] for k in keys} for x in response["items"]
                ]

            # Add the album_id back in
            [x.update({"album_id": album}) for x in album_result]
            result.extend(album_result)

        # Remove all other info about artists except ids
        for i in range(len(result)):
            result[i]["artists"] = [x["id"] for x in result[i]["artists"]]

        return result

    def get_track_features(self, tracks: List) -> Dict:
        """
        Returns a tracks info and audio features
        """
        # Call info
        id_lim = 50
        keys = [
            "id",
            "danceability",
            "energy",
            "key",
            "loudness",
            "mode",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
            "tempo",
            "time_signature",
        ]
        batches = np.array_split(tracks, int(np.ceil(len(tracks) / id_lim)))

        # Get track features in batches
        result = []
        for batch in tqdm(batches):
            self._authenticate()
            response = self.sp.audio_features(batch)
            result.extend([{k: x[k] for k in keys} for x in response if x is not None])

        # Filter to just ids
        return result

    def get_track_audio_analysis(self, tracks:List) -> Dict:
        """
        Gets the detailed audio analysis for a track
        """

        pass