import spotipy
from spotipy import util
from spotipy import oauth2
import numpy as np
import pandas as pd
from tqdm import tqdm
import os
import datetime as dt
import time
import json
import datetime as dt

# DB
from pymongo import MongoClient

# ENV
from dotenv import load_dotenv

load_dotenv()

# INTERNAL
from .db import *
from .storm_client import *
from .runner import *
from .analytics import *


class Storm:
    """
    Main callable that initiates and saves storm data
    """

    def __init__(self, storm_names, start_date=None):

        self.print_initial_screen()
        self.storm_names = storm_names
        # self.sac = StormAnalyticsController()

    def print_initial_screen(self):

        print("A Storm is Brewing. . .\n")
        time.sleep(0.5)

    def Run(self):

        print("Spinning up Storm Runners. . . ")
        for storm_name in self.storm_names:
            StormRunner(storm_name).Run()

        print("Done Runnings, rebuilding storm_analytics")
        # self.sac.analytics_pipeline()


# Storm(['film_vg_instrumental', 'contemporary_lyrical']).Run()
