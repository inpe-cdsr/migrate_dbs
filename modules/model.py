# -*- coding: utf-8 -*-

from json import dumps

from pandas import read_sql, to_datetime
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from modules.environment import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, \
                                MYSQL_PORT, MYSQL_DATABASE
from modules.logging import logging


class MySQLConnection():

    def __init__(self):
        self.engine = None

    def connect(self):
        try:
            self.engine = create_engine(
                (f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@'
                f'{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
            )

        except SQLAlchemyError as error:
            error_message = 'An error occurred during database connection'

            logging.error('MySQLConnection.connect() - error.code: %s', error.code)
            logging.error('MySQLConnection.connect() - error.args: %s', error.args)
            logging.error('MySQLConnection.connect() - %s: %s\n', error_message, error)

            # error_message += ': ' + str(error.args)

            self.close()
            raise Exception(error_message)

    def close(self):
        if self.engine is not None:
            self.engine.dispose()

        self.engine = None

    def try_to_connect(self):
        attempt = 0

        # while engine is None, try to connect
        while self.engine is None and attempt < 3:
            attempt += 1
            self.connect()

        if attempt >= 3:
            self.close()
            raise Exception('Connection was not opened to the database.')

    def execute(self, query):
        logging.info('MySQLConnection.execute()')

        try:
            logging.info('MySQLConnection.execute() - query: %s\n', query)

            self.try_to_connect()

            df = read_sql(query, con=self.engine)

            return df

        except SQLAlchemyError as error:
            # self.rollback()
            error_message = 'An error occurred during query execution'

            logging.error('MySQLConnection.execute() - error.code: %s', error.code)
            logging.error('MySQLConnection.execute() - error.args: %s', error.args)
            logging.error('MySQLConnection.execute() - %s: %s\n', error_message, error)

            error_message += ': ' + str(error.args)

            raise Exception(error_message)

        # finally is always executed (both at try and except)
        finally:
            self.close()

    def select_from_collection(self):
        return self.execute('SELECT * FROM stac_collection;')

    def select_from_item(self):
        return self.execute('SELECT * FROM stac_item;')


class PostgreSQLConnection():

    def __init__(self):
        try:
            # the elements for connection are got by environment variables
            self.engine = create_engine('postgresql+psycopg2://')

        except SQLAlchemyError as error:
            logging.error(f'PostgreSQLConnection.__init__() - An error occurred during engine creation.')
            logging.error(f'PostgreSQLConnection.__init__() - error.code: {error.code} - error.args: {error.args}')
            logging.error(f'PostgreSQLConnection.__init__() - error: {error}\n')

            raise SQLAlchemyError(error)

    def execute(self, query, params=None, is_transaction=False):
        logging.debug('PostgreSQLConnection.execute()')
        logging.debug(f'PostgreSQLConnection.execute() - is_transaction: {is_transaction}')
        logging.debug(f'PostgreSQLConnection.execute() - query: {query}')
        logging.debug(f'PostgreSQLConnection.execute() - params: {params}')

        try:
            if is_transaction:
                with self.engine.begin() as connection:  # runs a transaction
                    connection.execute(query, params)
                return

            # SELECT
            # with self.engine.connect() as connection:
            #     # safe code against SQL injection - https://realpython.com/prevent-python-sql-injection/
            #     result = connection.execute("SELECT admin FROM users WHERE username = %(username)s", {'username': username});
            #     for row in result:
            #         print("username:", row['username'])

        except SQLAlchemyError as error:
            logging.error(f'PostgreSQLConnection.execute() - An error occurred during query execution.')
            logging.error(f'PostgreSQLConnection.execute() - error.code: {error.code} - error.args: {error.args}')
            logging.error(f'PostgreSQLConnection.execute() - error: {error}\n')

            raise SQLAlchemyError(error)

    ####################################################################################################
    # BAND
    ####################################################################################################

    def insert_into_bands(self, id=None, name=None, common_name=None, description=None, min_value=None,
                          max_value=None, resolution=None, metadata=None, resolution_unit_id=None, **kwards):

        query = (
            'INSERT INTO bdc.bands (id, name, common_name, description, min_value, '
            'max_value, resolution_x, resolution_y, metadata, resolution_unit_id) '
            'VALUES (%(id)s, %(name)s, %(common_name)s,  %(description)s, %(min_value)s, '
            '%(max_value)s, %(resolution)s, %(resolution)s, %(metadata)s, %(resolution_unit_id)s);'
        )

        self.execute(
            query,
            params={
                'id': id,
                'name': name,
                'common_name': common_name,
                'description': description,
                'min_value': min_value,
                'max_value': max_value,
                'resolution': resolution,
                'metadata': metadata,
                'resolution_unit_id': resolution_unit_id
            },
            is_transaction=True
        )

    ####################################################################################################
    # COLLECTION
    ####################################################################################################

    def insert_into_collections(self, id=None, name=None, description=None, start_date=None, end_date=None,
                               min_x=None, min_y=None, max_x=None, max_y=None, **kwards):

        query = (
            'INSERT INTO bdc.collections '
            '(id, name, title, description, start_date, end_date, extent) '
            'VALUES '
            '(%(id)s, %(name)s, %(title)s, %(description)s, %(start_date)s, %(end_date)s, '
            'ST_MakeEnvelope(%(min_x)s, %(min_y)s, %(max_x)s, %(max_y)s, 4326));'
        )

        self.execute(
            query,
            params={
                'id': id,
                'name': name,
                'title': name,
                'description': description,
                'start_date': start_date,
                'end_date': end_date,
                'min_x': min_x,
                'min_y': min_y,
                'max_x': max_x,
                'max_y': max_y
            },
            is_transaction=True
        )

    ####################################################################################################
    # ITEM
    ####################################################################################################

    def insert_into_items(self, id=None, name=None, collection_id=None, datetime=None, cloud_cover=None,
                          path=None, row=None, satellite=None, sensor=None, sync_loss=None,
                          assets=None, deleted=None, srid=4326,
                          bl_longitude=None, bl_latitude=None, tr_longitude=None, tr_latitude=None, **kwards):

        metadata = {
            # 'datetime': datetime,
            # 'date': date,
            'path': path,
            'row': row,
            'satellite': satellite,
            'sensor': sensor,
            # 'cloud_cover': cloud_cover,
            'sync_loss': sync_loss,
            'deleted': deleted
        }

        query = (
            'INSERT INTO bdc.items '
            '(id, name, collection_id, start_date, end_date, cloud_cover, '
            'assets, metadata, geom, min_convex_hull, srid) '
            'VALUES '
            '(%(id)s, %(name)s, %(collection_id)s, %(datetime)s, %(datetime)s, %(cloud_cover)s, '
            '%(assets)s,  %(metadata)s, '
            'ST_MakeEnvelope(%(min_x)s, %(min_y)s, %(max_x)s, %(max_y)s, %(srid)s), '
            'ST_MakeEnvelope(%(min_x)s, %(min_y)s, %(max_x)s, %(max_y)s, %(srid)s), %(srid)s);'
        )

        self.execute(
            query,
            params={
                'id': id,
                'name': name,
                'collection_id': collection_id,
                'datetime': datetime,
                'cloud_cover': cloud_cover,
                'assets': assets,
                'metadata': dumps(metadata),
                'srid': srid,
                'min_x': bl_longitude,
                'min_y': bl_latitude,
                'max_x': tr_longitude,
                'max_y': tr_latitude
            },
            is_transaction=True
        )

    ####################################################################################################
    # RESOLUTION
    ####################################################################################################

    def insert_into_resolution(self, id=None, name=None, symbol=None, **kwards):

        query = (
            'INSERT INTO bdc.resolution_unit (id, name, symbol) '
            'VALUES (%(id)s, %(name)s, %(symbol)s);'
        )

        self.execute(
            query,
            params={
                'id': id,
                'name': name,
                'symbol': symbol
            },
            is_transaction=True
        )

    ####################################################################################################
    # GENERIC
    ####################################################################################################

    def delete_from_table(self, table):
        self.execute(f'DELETE FROM {table};', is_transaction=True)

