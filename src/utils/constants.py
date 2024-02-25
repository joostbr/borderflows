import os.path

import pytz

LOCALTZ = pytz.timezone('Europe/Brussels')

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
CONFIG_PATH = os.path.join(ROOT_PATH, "config")