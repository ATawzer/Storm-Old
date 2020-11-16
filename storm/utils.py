import spotipy
from spotipy import util
from spotipy import oauth2
import numpy as np
import pandas as pd
from tqdm import tqdm_notebook as tqdm
from os import path
import datetime as dt
import time

# A class to manage all of the storm functions and authentication
class Storm:
    """
    Single object for running and saving data frm the storm run. Call Storm.Run() to generate a playlist from
    saved artists.
    """
    def __init__(self, user_id, inputs, output, archive, name, start_date=None, filter_unseen=True):
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
        self.client_id = '9b41900f606c4e55855524f448917d64' # API app id
        self.client_secret = '3277c16b708548369ce1f42deed974ea' # API app secret
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
        new_artists = []
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