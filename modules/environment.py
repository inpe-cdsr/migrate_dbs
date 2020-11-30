# -*- coding: utf-8 -*-

"""Get environment variables"""


from os import environ
from logging import DEBUG, INFO

from modules.utils import str2bool


os_environ_get = environ.get


DEBUG_MODE = str2bool(os_environ_get('DEBUG_MODE', 'True'))

DATA_PATH = os_environ_get('DATA_PATH', 'assets/data/')
DATA_FIXED_PATH = os_environ_get('DATA_FIXED_PATH', 'assets/data_fixed/')

# MYSQL connection
MYSQL_USER = os_environ_get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os_environ_get('MYSQL_PASSWORD', 'password')
MYSQL_HOST = os_environ_get('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os_environ_get('MYSQL_PORT', 9004))
MYSQL_DATABASE = os_environ_get('MYSQL_DATABASE', 'catalog_rubi')

# Postgres connection
# PGUSER = os_environ_get('PGUSER', 'root')
# PGPASSWORD = os_environ_get('PGPASSWORD', 'password')
# PGHOST = os_environ_get('PGHOST', 'localhost')
# PGPORT = int(os_environ_get('PGPORT', 9004))
# PGDATABASE = os_environ_get('PGDATABASE', 'catalog')

# default logging level in production server
LOGGING_LEVEL = INFO

# if the application is in development mode, then change the logging level and debug mode
if DEBUG_MODE:
    LOGGING_LEVEL = DEBUG
