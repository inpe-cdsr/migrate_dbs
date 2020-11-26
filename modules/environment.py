# -*- coding: utf-8 -*-

"""Get environment variables"""


from os import environ

os_environ_get = environ.get


DATA_PATH = os_environ_get('DATA_PATH', 'assets/data/')

# MYSQL connection
MYSQL_USER = os_environ_get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os_environ_get('MYSQL_PASSWORD', 'password')
MYSQL_HOST = os_environ_get('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os_environ_get('MYSQL_PORT', 9004))
MYSQL_DATABASE = os_environ_get('MYSQL_DATABASE', 'catalog_rubi')

# Postgres connection
# POSTGRES_USER = os_environ_get('POSTGRES_USER', 'root')
# POSTGRES_PASSWORD = os_environ_get('POSTGRES_PASSWORD', 'password')
# POSTGRES_HOST = os_environ_get('POSTGRES_HOST', 'localhost')
# POSTGRES_PORT = int(os_environ_get('POSTGRES_PORT', 9004))
# POSTGRES_DATABASE = os_environ_get('POSTGRES_DATABASE', 'catalog')
