from dotenv import load_dotenv
import sys

load_dotenv()

# Internal
from src.db import *
from src.runner import *
from src.analytics import *
from src.weatherboy import *
from src.storm import Storm

# StormRunner('contemporary_lyrical_v2').Run()
# StormRunner('film_vg_instrumental_v2').Run()

root = logging.getLogger("storm")
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)

sdb = StormDB()
sc = StormClient(1241528689)

tracks = sdb.get_tracks()
num_tracks = len(tracks)

batches = np.array_split(tracks, int(np.ceil(num_tracks / 5000)))
for i, batch in enumerate(batches):

    l.debug(f"Collecting Tracks {i*5000} - {(i+1)*5000} of {num_tracks}")
    featurized_tracks = sc.get_track_features(batch)
    sdb.update_track_features(featurized_tracks)
