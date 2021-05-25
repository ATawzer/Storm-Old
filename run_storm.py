from dotenv import load_dotenv
load_dotenv()

# Internal
from src.db import *
from src.analytics import *
from src.storm import Storm

if __name__=="__main__":
    Storm(['contemporary_lyrical', 'film_vg_instrumental']).Run()