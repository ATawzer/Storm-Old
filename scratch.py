storm = Storm(['film_vg_instrumental'])

sr = StormRunner('film_vg_instrumental')
sr.prepare_playlists()


sc = StormClient('1241528689')
test = sc.get_artists_from_tracks([])
