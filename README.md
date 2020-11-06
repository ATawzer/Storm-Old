# The Storm
A Spotify App designed for exhaustive music discovery. Using a set of user-defined playlists, The Storm will scour the Spotify library for all the new releases from the artists of that playlist. There is no following or manual selection needed from the user, just a playlist of the music they already like. All releases within a given window will then be sent directly to the output playlist the user desires, ready for listening.

# Motivation
The Storm is titled after the massive amounts of new music being added and listened to on Spotify everyday. This Repo is dedicated to giving users a way to navigate through that storm and find the music they want from the artists and playlists they already frequent all while requiring as little input from the user as possible. However, The Storm isn’t perfect and can be reasonably intimidating itself. Continue through the readme to determine if the Storm is a good fit for your musical listening needs.

Spotify currently employs many different ways of delivering users new music. Here is an overview of the current ways users could find new music:
- Release radar: 30 new tracks delivered to the user weekly
- Curated Playlists: Pop Rising and New Music Friday are among the most evident playlists that Spotify curates to contain new music frm popular artists
- Radios: Radios can be started from tracks, artists or even playlists of music. They hope to find similar music to deliver to you
- Recommended tracks: These appear beneath playlists and are meant to give you a pick of music close to the songs in the playlist
- Discover Weekly: A variant of release radar focusing on new artists or infrequently played artists designed to broaden a user’s musical horizons

These are mostly effective ways of finding new music. However, particularly in niche genres of music or particular user tastes, these methods are often insufficient and deliver content that is disconnected from a users taste. This is likely due to the audio features available to tracks being not quite descriptive enough to really understand what it is that a user likes. Outlined below are a few issues with each of the above methods and how The Storm addresses them.

### Release Radar	
- Problem: 30 tracks is not nearly enough to cover the entire base of a users liked artists. Additionally, when artists release lots of music only 1 or 2 of the tracks actually make it into this playlist. The 30 that do make it in often are tied to the most popular artist, not necessarily the user’s favorite.
- Solution: The Storm has no limits, if a user tracks lots of artists, every artist will appear in that windows storm. The user can be positive no release will slip through the cracks.
### Curated Playlists
- Problem: Manually maintained playlists mean a large user group must be happy with the tracks to justify the effort. Thus, the more refined taste a music-lover has, the more specific and less likely a playlist exists to match the music they like
- Solution: Storm uses the playlists a user already frequents to generate the tracks for the user. Since it’s based on the user preference and is not limited, the user gets music only from artists they care about. Everyone could use their own storm, reducing the need for the output to be highly curated.
### Radios
- Problem: While great to have on in the background, users actively looking for music will hear many of the same tracks they already know. Radios, also, have no obvious logic, meaning it would be nearly impossible to ensure all the music you are looking for would eventually make its way through a radio
- Solution: Storm focuses exclusively on music the user hasn’t heard, so while it might not be great for background listening it actively monitors which tracks the user has already listened to and when they were released. This logic is transparent and allows the user to know which songs they missed and even stores the new tracks they didn’t get around to on subsequent storm runs. Using the blacklist feature, users can keep track of artists they do not want to receive music for, ensuring the thumbs-down benefit of a radio is still present.
### Recommended Tracks
- Problem: In large playlists, 10-20 tracks simply does not cover the amount of music the user could potentially be tapping in to. The tracks are also heavily influenced by the popularity of a track in the sub-genre the playlist fits into.
- Solution: Storm delivers many tracks, paying attention to every artist in the playlist. Users know the recommendation is based solely on the artists they already enjoy so new tracks are not a popularity contest but rather an artist fit.
### Discover Weekly
- Problem: Once again, the number of tracks is limited here to only 30, and the artists are often quite obscure, not connected with the users preferences.
- Solution: Storm exploits the newest music trend: Features. The Storm pays attention to all artists on a track and if you like songs with more than one artist, meaning the collaborators of your top artists are also tracked. This makes it easy to expand musical horizons and does so in parallel with the artists who create the music you already love.

# Setup and Usage

To get started, you will need a few pieces of information. The .ipynb notebook titled Storm Notebook has a cell dedicated to setting this information up. As a user of the storm I wanted to share some sample visuals to track how useful the storm has been to me. Ove the course of its creation I have tried out 60,000+ tracks across 1,400+ artists all orchestrated using the storm. 



I almost always know and listen to songs before any of my friends or family, even the true fans of certain artists. I also know I won’t ever miss the best music out there from my favorite artists.


## 1. User Id 
The first and arguably most important is your user id, which can be found by navigating to your profile and clicking the three dots icon. Then, navigate to Share -> Copy Spotify URI and you should be presented with something like the following:

spotify:user:1241528689

The code in the last piece (usually a number) is your user id. Paste this number as the user_id variable

** Special Note **
On the first run, Spotify will need to authenticate and ask for your permission to let The Storm access your data. See storm.utils.py for the exact web API scope names. Storm only needs permission to read and write playlists on your account. The user ids are only stored in your local copy of the notebook and all authentication / security is handled by Spotify, not this application.

## 2. Output and Archive Playlists
Next, you must either pick a playlist or create one dedicated to saving the results of the Storm run. I, for instance, use a playlist simply titled The Storm, which is where I save all the music foud for me. The archive playlist is where songs you haven’t listened to will be stored if another storm run is performed. I have labeled this The Storm Archive in my own playlist library. Add the output and archive playlist ids into the dictionary under ‘daily’ and ‘archive’ keys respectively.

Obtaining the playlist id is similar to the above step. On an existing or freshly created playlist right-click the playlist -> Share -> Copy Spotify URI. Grab the numeric code at the end as the playlist id

## 3. Input playlists
This is where your preferences are inputted. Find a playlist or create one that has the music from the artists you absolutely love. Add this into the dictionary as ‘name’:’id’. Storm will use this playlist to obtain an artist list to get tracks for. Additionally, if there are curated playlists you are interested in following, add them in! There is no limit to the number of playlists storm can track. Though, for run-time purposes, it is usually best to start with a succinct playlist featuring only the best of the artists you follow.

## 4. Ongoing usage
It is up to you on how best to go through all the tracks. I typically use a move-or-delete workflow. I have a playlist where I store all of the tracks from the storm I am interested in. As I listen to the storm, if I like a track I move it and if I don’t I delete it. Don’t worry, the storm knows which track it was, so you’ll know you can find it again. Otherwise, the tracks you liked are stored and the ones you didn’t are gone. 

If certain artists post to much noise, add their URI into the blacklist.csv file and they will no longer bother you. Some artists represent aggregates of artists and have to be blacklisted to prevent overwhelming you. 

To start a new storm or refresh your artist list simply change the name of the storm and it will be like starting anew.

** Special Note **
Make sure you change the name of the storm the first time around. Do this anytime you want to change the artists you are tracking. Metadata and API acceleration is stored to this name and embedded into the .csv’s that store your run data.

# Requirements

See requirements.txt

# Final Thoughts

## 1. Future Improvement
- Better ranking of delivered music
- Better de-duping of same tracks released in different languages
- More use of audio features to remove bad or spammy artists from having to manually blacklisted
- Interface for orchestrating the storm outside the notebook
- Support for multiple storm runs with one .Run()
- Allow user to specify if they want to see all new tracks or only those in the release window (currently will show both the first time an artist is added)

## 2. Known Issues / Fixes
- Timeout token issues if rerunning within the hour. Storm doesn’t know when token was started and thus assumes it has more time than it does. Simply rerun
- 10,000 track limit on playlist writing. Use a smaller set of artists or a smaller time window on releases. The first pass on an artist gets a lot more
- Max retries error occurs on some artists. Try changing up artists or just reload the notebook (often fixes it)

## 3. Helpful Links
- Spotify Web API: https://developer.spotify.com/documentation/web-api/
- Spotipy documentation: https://spotipy.readthedocs.io/en/2.16.1/

Feel free to reach out with any suggested improvements!

