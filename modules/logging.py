# -*- coding: utf-8 -*-

import logging

from modules.environment import LOGGING_LEVEL


logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=LOGGING_LEVEL)
