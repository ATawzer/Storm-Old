import time
import sys

def slow_print(string='', t=.01):
    for letter in string:
        sys.stdout.write(letter)
        time.sleep(t)
    sys.stdout.write('\n')
