from dotenv import load_dotenv
import sys

load_dotenv()

# Internal
from src.db import *
from src.runner import *
from src.analytics import *
from src.weatherboy import *
from src.storm import Storm

# Test 1 - Successful Connections and syntax - Clients
try:
    StormClient(1241528689)
    StormUserClient(1241528689)

    print("Test 1 Success!")

except:

    print("Failure with Client Connection and Initilization.")


# Test 2 - Database Initiliazation
try:
    list(StormDB().artists.find({"_id":"123"}))

    print("Test 2 Success!")

except:

    print("Failure with MongoDB Database Connection and Initilization.")
