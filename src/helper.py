import time
import sys

def slow_print(string='', t=.0001):
    for letter in string:
        sys.stdout.write(letter)
        time.sleep(t)
    print()
