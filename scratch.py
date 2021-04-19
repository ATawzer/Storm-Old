storm = Storm(['film_vg_instrumental'])

sr = StormRunner('film_vg_instrumental')
sr.collect_playlist_info()
sr.collect_artist_info()

sdb = StormDB()
sdb.get_loaded_playlist_tracks('0R1gw1JbcOFD0r8IzrbtYP')



sc = StormClient('1241528689')
test = sc.get_artist_info(["0360rTDeUjEyBXaz2Ki00a",
"07vycW8ICLf5hKb22PFWXw",
"0HDxlFsXwyrpufs4YgTNMm",
"0InzETPzx4u2fVgldqQOcd",
"0QxmfaZ2M3gLqL3f7Tap8r",
"0UM4gJJKawZSZuJxYcIwJS",
"0UncJfL7Vqvm9WFuWQSVBC",
"0YC192cP3KPCRWx8zr8MfZ",
"0Z6bE6kOVhh2DHZPMUz2Sr",
"0bdJp8l3a1uJRKe2YaAcE9"])
