# Internal
from src.helper import *
from src.storm_client import Storm
print = slow_print # for fun

# ENV
from dotenv import load_dotenv
load_dotenv()


Storm(['contemporary_lyrical']).Run()


test = StormDB().get_last_run('film_vg_instrumental')