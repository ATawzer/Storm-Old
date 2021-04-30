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

# DB
from pymongo import MongoClient

# ENV
from dotenv import load_dotenv
load_dotenv()

# INTERNAL
from .db import *
from .storm_client import *
from .runner import *

class Storm:
    """
    Main callable that initiates and saves storm data
    """
    def __init__(self, storm_names, start_date=None):

        self.print_initial_screen()
        self.storm_names = storm_names

    def print_initial_screen(self):

        print("A Storm is Brewing. . .\n")
        time.sleep(.5)
        
    def Run(self):

        print("Spinning up Storm Runners. . . ")
        for storm_name in self.storm_names:
            StormRunner(storm_name).Run()

#Storm(['film_vg_instrumental', 'contemporary_lyrical']).Run()