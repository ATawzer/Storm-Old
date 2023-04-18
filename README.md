# The Storm
Exhaustive music discovery in Spotify. Using a set of user-defined playlists, The Storm will scour the Spotify library for all the new releases from the artists of that playlist. There is no following or manual selection needed from the user, just a playlist of the music they already like. All releases within a given window will then be sent directly to the output playlist the user desires, ready for listening.

# Setup

## 1. Environment
- Run `pipenv install` inside the repo to install all necessary packages
- Install MongoDB, this can be done from the mongodb website according to your OS: https://www.mongodb.com/
- Configure a .env file with your information
    - Create an API app, https://developer.spotify.com/documentation/web-api/. Use the Client and Secret for .env
    - Find your Spotify User ID
    - Create a database in MongoDB to authenticate against and store data
- Make sure your local MongoDB server is running by running `mongod`

sample .env file, remove the comments from each line
```
    storm_client_id=SPOTIFY_CLIENT # Get this from Spotify API by making an API app
    storm_client_secret=SPOTIFY_SECRET # Get this from Spotify API by making an API app

    spotify_user_id=SPOTIFY_USER_ID # This is your Spotify User ID, you can find this in your User link or your User URI

    mongo_host=127.0.0.1:27017 # Default
    mongo_user=USERNAME # Configure this in the Admin database of MongoDB
    mongo_pass=PASSWORD # Configure this in the Admin database of MongoDB
    mongo_db=storm # Authentication will happen against this database and all data will be stored here
```

# Helpful Links
- Spotify Web API: https://developer.spotify.com/documentation/web-api/
- Spotipy documentation: https://spotipy.readthedocs.io/en/2.16.1/

Feel free to reach out if you would like to start using this code.

