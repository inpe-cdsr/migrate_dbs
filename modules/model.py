# -*- coding: utf-8 -*-

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

    def __del__(self):
        self.engine.dispose()

    def execute(self, query, params=None, is_transaction=False):
        logging.info('PostgreSQLConnection.execute()')
        logging.error(f'PostgreSQLConnection.execute() - is_transaction: {is_transaction}')
        logging.error(f'PostgreSQLConnection.execute() - query: {query}\n')

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
    # COLLECTION
    ####################################################################################################

    def insert_into_collection(self, id=None, name=None, description=None, start_date=None, end_date=None,
                          min_y=None, min_x=None, max_y=None, max_x=None, **kwards):

        query = ('INSERT INTO collections (id, name, title, description, start_date, end_date, extent)'
                 'VALUES (%(id)s, %(name)s, %(title)s, %(description)s, %(start_date)s, %(end_date)s, %(extent)s);')

        extent_spatial = f"ST_Polygon('LINESTRING({min_y} {min_x}, {max_y} {max_x})'::geometry, 4326)"

        self.execute(
            query,
            params={
                'id': id,
                'name': name,
                'title': name,
                'description': description,
                'start_date': start_date,
                'end_date': end_date,
                'extent': extent_spatial
            },
            is_transaction=True
        )

    ####################################################################################################
    # ITEM
    ####################################################################################################

    ####################################################################################################
    # GENERIC
    ####################################################################################################

    def delete_from_table(self, table):
        self.execute(f'DELETE FROM {table};', is_transaction=True)

