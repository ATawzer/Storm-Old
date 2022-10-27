# The Storm
A Spotify App designed for exhaustive music discovery. Using a set of user-defined playlists, The Storm will scour the Spotify library for all the new releases from the artists of that playlist. There is no following or manual selection needed from the user, just a playlist of the music they already like. All releases within a given window will then be sent directly to the output playlist the user desires, ready for listening.

# Motivation and The Storm's Value to Music Lovers
The Storm is titled after the massive amounts of new music being added and listened to on Spotify everyday. This Repo is dedicated to giving users a way to navigate through that storm and find the music they want from the artists and playlists they already frequent all while requiring as little input from the user as possible. However, The Storm isn’t perfect and can be reasonably intimidating itself. Continue through the readme to determine if the Storm is a good fit for your musical listening needs.

Spotify currently employs many different ways of delivering users new music. Here is an overview of the current ways users could find new music:
- Release radar: 30 new tracks delivered to the user weekly
- Curated Playlists: Pop Rising and New Music Friday are among the most evident playlists that Spotify curates to contain new music frm popular artists
- Radios: Radios can be started from tracks, artists or even playlists of music. They hope to find similar music to deliver to you
- Recommended tracks: These appear beneath playlists and are meant to give you a pick of music close to the songs in the playlist
- Discover Weekly: A variant of release radar focusing on new artists or infrequently played artists designed to broaden a user’s musical horizons

These are mostly effective ways of finding new music. However, particularly in niche genres of music or particular user tastes, these methods can be improved with a personalized ML prediction system.

### Release Radar	
- Problem: 30 tracks does not cover the entire base of a users liked artists. Additionally, when artists release lots of music only 1 or 2 of the tracks actually make it into this playlist. The 30 that do make it in often are tied to the most popular artist, not necessarily the user’s favorite.
- Solution: The Storm has no limits, if a user tracks lots of artists, every artist will appear in that windows storm. The user can be positive no release will slip through the cracks. How refined the delivered tracks are is up to the user and will vary by week depending on the activity of the artists they like.
### Curated Playlists
- Problem: Manually maintained playlists mean a large user group must be happy with the tracks to justify the effort. Thus, the more refined taste a music-lover has, the more specific and less likely a playlist exists to match the music they like.
- Solution: Storm uses the playlists a user already frequents to generate the tracks for the user. Since it’s based on the user preference and is not limited, the user gets music only from artists they care about. Everyone could use their own storm, reducing the need for the output to be highly curated.
### Radios
- Problem: While great to have on in the background, users actively looking for music will hear many of the same tracks they already know. 
- Solution: Storm focuses exclusively on music the user hasn’t heard, so while it might not be great for background listening it actively monitors which tracks the user has already listened to and when they were released. This logic is transparent and allows the user to know which songs they missed and even stores the new tracks they didn’t get around to on subsequent storm runs. Using the blacklist feature, users can keep track of artists they do not want to receive music for, ensuring the thumbs-down benefit of a radio is still present.
### Recommended Tracks
- Problem: In large playlists, 10-20 tracks might not cover the amount of music the user could potentially be tapping in to. The tracks are also heavily influenced by the popularity of a track in the sub-genre the playlist fits into.
- Solution: Storm delivers many tracks, paying attention to every artist in the playlist. Users know the recommendation is based solely on the artists they already enjoy so new tracks are not a popularity contest but rather an artist and track fit. This might not deliver new recomendations as easily as spotify currently does though, so perhaps best used in conjunction.
### Discover Weekly
- Problem: The number of tracks is limited here to only 30. This concept seems promising, though I can't say I personally have had much luck.
- Solution: Perhaps alongside Discover Weekly Storm's prediction scores the playlist could expand into new artists and help refine the tracks worth looking at

# Setup and Usage

To get started, you will need:

- Spotify User ID
- Spotify Web API accesss (client and secret id)
- MongoDB database the storm can write to and from (local is fine)
- A specific Spotify playlist structure

Most of this information lives inside your own .env file that will be loaded when using. See class definitions for the exact information you will need.

The storm is still evolving and requires a lot to get off the ground. Hopefully by version 4 or 5 this will not be the case. Please contact me if you are interested in using it in its current form.

# Requirements

Storm uses pipenv. Simply run pipenv install inside this repo and all dependencies should be installed.

# Final Thoughts

## 1. Future Improvement
- Detailing setup
- Allowing for non-database runs (in case you don't want a backend or ML)
- automatic playlist creation when making a new storm

## 2. Known Issues / Fixes
- 

## 3. Helpful Links
- Spotify Web API: https://developer.spotify.com/documentation/web-api/
- Spotipy documentation: https://spotipy.readthedocs.io/en/2.16.1/

Feel free to reach out if you would like to start using this code.

