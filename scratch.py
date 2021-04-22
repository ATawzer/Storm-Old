storm = Storm(['film_vg_instrumental'])

sr = StormRunner('film_vg_instrumental')
sr.run_date = '2021-04-20'
sr.load_last_run()
sr.collect_playlist_info()
sr.collect_artist_info()
sr.collect_album_info()
#sr.collect_track_features()
sr.filter_storm_tracks()

sdb = StormDB()
sdb.get_blacklist('instrumental_blacklist')
test = sdb.get_tracks_for_feature_collection()
#sdb.get_albums_by_release_date('2021-04-01', '2021-04-05')


sdb = StormDB()
sdb.update_artist_albums()

sdb.artists.update_many({}, {"$unset":{"albums":1}})

test = sdb.get_tracks_for_feature_collection()[:5]
sc = StormClient('1241528689')
test_response = sc.get_track_features(test)
sdb.update_track_features(test_response)


test = sc.get_artist_albums(["0360rTDeUjEyBXaz2Ki00a",
"07vycW8ICLf5hKb22PFWXw",
"0HDxlFsXwyrpufs4YgTNMm",
"0InzETPzx4u2fVgldqQOcd",
"0QxmfaZ2M3gLqL3f7Tap8r",
"0UM4gJJKawZSZuJxYcIwJS",
"0UncJfL7Vqvm9WFuWQSVBC",
"0YC192cP3KPCRWx8zr8MfZ",
"0Z6bE6kOVhh2DHZPMUz2Sr",
"0bdJp8l3a1uJRKe2YaAcE9"])

sdb = StormDB()
sdb.update_artist_album_collected_date(["0360rTDeUjEyBXaz2Ki00a",
"07vycW8ICLf5hKb22PFWXw",
"0HDxlFsXwyrpufs4YgTNMm",
"0InzETPzx4u2fVgldqQOcd",
"0QxmfaZ2M3gLqL3f7Tap8r",
"0UM4gJJKawZSZuJxYcIwJS",
"0UncJfL7Vqvm9WFuWQSVBC",
"0YC192cP3KPCRWx8zr8MfZ",
"0Z6bE6kOVhh2DHZPMUz2Sr",
"0bdJp8l3a1uJRKe2YaAcE9"])

sdb.update_albums(test)

from_date = dt.datetime.strptime('2021-04-01', '%Y-%m-%d')
to_date = dt.datetime.strptime('2021-04-05', '%Y-%m-%d')

list(sdb.albums.find({"release_date": {"$gte": '2021-04-01', "$lt": '2021-04-05'}}))

# Putting scraped tracks on to their albums
sdb = StormDB()
q = {}
cols = {"last_updated":0}
r = list(sdb.tracks.find(q, cols))

for x in r:
    x["id"] = x.pop("_id")

sdb.update_tracks(r)
