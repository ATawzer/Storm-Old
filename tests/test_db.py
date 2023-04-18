import pytest

from storm import StormDB

@pytest.fixture
def storm_db():
    yield StormDB()

def test_update_tracks(storm_db):
    track_record = {
        '_id': '5f4f4f4f4f4f4f4f4f4f4f4f',
        'id': '5f4f4f4f4f4f4f4f4f4f4f4f',
        'name': 'The Way You Make Me Feel',
        'artists': ['Michael Jackson'],
        'album_id': '5f4f4f4f4f4f4f4f4f4f4f4f',
    }

    storm_db.update_tracks([track_record])

def test_get_track_info(storm_db):
    info = storm_db.get_track_info(['5f4f4f4f4f4f4f4f4f4f4f4f'], fields={'_id': 1, 'name': 1, 'artists': 1, 'album_id': 1})

    assert set(['_id', 'name', 'artists', 'album_id']).difference(info[0].keys()) == set()
    assert info[0]['_id'] == '5f4f4f4f4f4f4f4f4f4f4f4f'
    assert info[0]['name'] == 'The Way You Make Me Feel'
    assert info[0]['artists'] == ['Michael Jackson']
    assert info[0]['album_id'] == '5f4f4f4f4f4f4f4f4f4f4f4f'
    