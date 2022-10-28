from dotenv import load_dotenv
import sys

load_dotenv()

# Internal
from src.db import *
from src.runner import *
from src.analytics import *
from src.weatherboy import *

root = logging.getLogger("storm")
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)

sdb = StormDB()
sc = StormClient(1241528689)

tracks = sdb.get_tracks_for_audio_analysis()
num_tracks = len(tracks)

batches = np.array_split(tracks, int(np.ceil(num_tracks / 100)))
for i, batch in enumerate(batches):

    l.debug(f"Collecting Tracks {i*100} - {(i+1)*100} of {num_tracks}")
    featurized_tracks = sc.get_track_audio_analysis(batch)
    sdb.update_track_analysis(featurized_tracks)
