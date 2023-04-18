import sys
import logging

# Internal
from invoke import task

from storm.runner import StormRunner
from storm.modeling import *

# Make sure to add the models you want here
STORM_CONFIG = {
    'film_vg_instrumental_v2': {
        'model_name': 'film_vg_instrumental__distinct__track_feature__12__cdcc6394-52fb-4a07-a645-4c1e52156afe',
        'model_friendly_name': '{cluster_number} - Film, VG and Instrumental'
    },
    'contemporary_lyrical_v2': {
        'model_name': 'contemporary_lyrical__distinct__track_feature__12__00cce27b-ee65-451a-bb11-104b47a2c80f',
        'model_friendly_name': '{cluster_number} - Contemporary Lyrical'
    },
}

@task
def setup_logging(c, level='info'):
    """
    Setups logging for the project. Defaults to info level, is automatically called by run_all.
    """

    if level == 'info':
        log_level = logging.INFO
    elif level == 'debug':
        log_level = logging.DEBUG

    root = logging.getLogger("storm")
    root.setLevel(log_level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)

@task
def run(c, storm_name):
    """
    Runs a storm by name, assumes the mongo server is already running and logging is setup.
    """
    StormRunner(
        storm_name,
        **STORM_CONFIG[storm_name]
    ).Run()

@task
def run_all(c):
    """
    Run all the configured storms, turning on the mongo server and shutting it down when done.
    """

    c.run('mongod')
    setup_logging(c)
    for storm_name in STORM_CONFIG:
        run(c, storm_name)

    c.run('mongo --eval "db.shutdownServer()"')

@task
def test(c):
    """
    Runs the tests with a coverage report.
    """
    c.run('pytest --cov=storm --cov-report=term-missing -x -p no:warnings')

