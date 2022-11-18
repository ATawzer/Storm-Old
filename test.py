from dotenv import load_dotenv
import sys

load_dotenv()

# Internal
from src.db import *
from src.runner import *
from src.analytics import *
from src.weatherboy import *
from src.modeling import ModelManager
from src.storm_client import *

print(StormUserClient(1241528689).get_user_playlists())

