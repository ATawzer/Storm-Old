from dotenv import load_dotenv
import sys

load_dotenv()

# Internal
from src.db import *
from src.runner import *
from src.analytics import *
from src.weatherboy import *
from src.modeling import ModelManager


ModelManager()
print("ModelManager Created")

# Test 1
print("Test 1 - Storm Client Connections and Syntax")
StormClient(1241528689)
StormUserClient(1241528689)
print("Test 1 Success!")

# Test 2
print("Test 2 - MongoDB Connections and Syntax")
StormDB()
print("Test 2 Success!")


