# -*- coding: utf-8 -*-

from pandas import read_sql, to_datetime
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from modules.environment import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, \
                                MYSQL_PORT, MYSQL_DATABASE
from modules.logging import logging


class DatabaseConnection():

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

            logging.error('DatabaseConnection.connect() - error.code: %s', error.code)
            logging.error('DatabaseConnection.connect() - error.args: %s', error.args)
            logging.error('DatabaseConnection.connect() - %s: %s\n', error_message, error)

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
        logging.info('DatabaseConnection.execute()\n')

        try:
            logging.info('DatabaseConnection.execute() - query: %s\n', query)

            self.try_to_connect()

            df = read_sql(query, con=self.engine)

            # logging.info('DatabaseConnection.execute() - df.head(): \n%s\n', df.head())
            # logging.info('DatabaseConnection.execute() - df.shape: %s\n', df.shape)
            # logging.info('DatabaseConnection.execute() - df.dtypes: \n%s\n', df.dtypes)

            return df

        except SQLAlchemyError as error:
            # self.rollback()
            error_message = 'An error occurred during query execution'

            logging.error('DatabaseConnection.execute() - error.code: %s', error.code)
            logging.error('DatabaseConnection.execute() - error.args: %s', error.args)
            logging.error('DatabaseConnection.execute() - %s: %s\n', error_message, error)

            error_message += ': ' + str(error.args)

            raise Exception(error_message)

        # finally is always executed (both at try and except)
        finally:
            self.close()

    def select_from_stac_collection(self):
        return self.execute('SELECT * FROM stac_collection;')

    def select_from_stac_item(self):
        return self.execute('SELECT * FROM stac_item;')