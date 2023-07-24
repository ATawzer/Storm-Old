from storm.storm_client import StormClient
import pytest
import os

@pytest.fixture
def storm_client():
    yield StormClient(user_id=os.getenv('spotify_user_id'))

def test_get_playlist_info(storm_client):
    playlist_info = storm_client.get_playlist_info('2zngrEiplX6Z1aAaIWgZ4m')

    assert 'description' in playlist_info
    assert 'id' in playlist_info
    assert 'name' in playlist_info
    assert 'owner' in playlist_info
    assert 'snapshot_id' in playlist_info

def test_get_playlist_tracks(storm_client):
    playlist_tracks = storm_client.get_playlist_tracks('2zngrEiplX6Z1aAaIWgZ4m')  # use a real playlist_id

    assert isinstance(playlist_tracks, list)
    assert len(playlist_tracks) > 0

def test_get_artists_from_tracks(storm_client):
    tracks = ['3NPhVitPBsJnXkJeMvjNb2', '3zrX6izmn310lKIUjOG9eL']
    artists = storm_client.get_artists_from_tracks(tracks)

    assert isinstance(artists, list)
    assert len(artists) > 0

def test_get_artist_info(storm_client):
    artists = ['2RQXRUsr4IW1f3mKyKsy4B', '5BxcZnUcETSt90VlbsdugI']
    artist_info = storm_client.get_artist_info(artists)

    assert isinstance(artist_info, list)
    assert len(artist_info) > 0

def test_get_artist_albums(storm_client):
    artists = ['2RQXRUsr4IW1f3mKyKsy4B', '5BxcZnUcETSt90VlbsdugI']
    artist_albums = storm_client.get_artist_albums(artists)

    assert isinstance(artist_albums, list)
    assert len(artist_albums) > 0

def test_get_album_tracks(storm_client):
    albums = ['0LgdvD2Xy58iSm87rEWhBm', '6VMrXUabbY8cFWLougHU5F']
    album_tracks = storm_client.get_album_tracks(albums)

    assert isinstance(album_tracks, list)
    assert len(album_tracks) > 0

def test_get_track_features(storm_client):
    tracks = ['3NPhVitPBsJnXkJeMvjNb2', '3zrX6izmn310lKIUjOG9eL']
    track_features = storm_client.get_track_features(tracks)

    assert isinstance(track_features, list)
    assert len(track_features) > 0

def test_get_track_audio_analysis(storm_client):
    tracks = ['3NPhVitPBsJnXkJeMvjNb2', '3zrX6izmn310lKIUjOG9eL']
    track_audio_analysis = storm_client.get_track_audio_analysis(tracks)

    assert isinstance(track_audio_analysis, list)
    assert len(track_audio_analysis) > 0
