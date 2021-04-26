# Internal
from src.helper import *
from src.storm_client import Storm
print = slow_print # for fun

# ENV
from dotenv import load_dotenv
load_dotenv()

Storm(['film_vg_instrumental', 'contemporary_lyrical']).Run()
