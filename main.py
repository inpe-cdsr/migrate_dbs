#!/usr/bin/env python
# -*- coding: utf-8 -*-

from json import dumps, loads
from pandas import read_csv, to_datetime

from modules.environment import DATA_PATH
from modules.logging import logging
from modules.model import MySQLConnection, PostgreSQLConnection


def fix_assets(row):
    """Fix column `assets` and merge `thumbnail` and `assets` columns"""

    thumbnail, assets = row

    new_assets = {}

    # convert from `str` to `dict` and iterate over it
    for asset in loads(assets):
        new_assets[asset['band']] = {
            'href': asset['href'],
            'type': 'image/tiff; application=geotiff',
        }
        new_assets[asset['band'] + '_xml'] = {
            'href': asset['href'].replace('.tif', '.xml'),
            'type': 'application/xml'
        }

    # add `thumbnail` to dict of assets
    new_assets['thumbnail'] = {
        'href': thumbnail,
        'type': 'image/png'
    }

    # convert from `dict` to `str`
    return dumps(new_assets)


def generate_collection_id_column(collection_name, df_collection):
    # get the collection from dataframe based on name
    collection = df_collection[(df_collection.name == collection_name)]

    # get the collection id and return
    return collection.iloc[0]['id']


class MigrateDBs():

    # def __init__(self):
        # PostgreSQL connection
        # self.db_postgres = PostgreSQLConnection()

    ##################################################
    # get the dataframes
    ##################################################

    def __get_dfs_from_mysqldb(self):
        # MySQL connection
        db_mysql = MySQLConnection()

        # get the dfs from database
        self.df_collection = db_mysql.select_from_collection()
        self.df_item = db_mysql.select_from_item()

        # save the dataframes in CSV files
        self.__save_dfs()

    def __get_dfs_from_csv_files(self, collection_file_name='collection.csv', item_file_name='item.csv'):
        # get the dfs from CSV files
        self.df_collection = read_csv(DATA_PATH + collection_file_name)
        self.df_item = read_csv(DATA_PATH + item_file_name)

    ##################################################
    # other
    ##################################################

    def __save_dfs(self, collection_file_name='collection.csv', item_file_name='item.csv'):
        """Save the dataframes in CSV files"""

        self.df_collection.to_csv(DATA_PATH + collection_file_name, index=False)
        self.df_item.to_csv(DATA_PATH + item_file_name, index=False)

        logging.info(f'`{collection_file_name}` and `{item_file_name}`'
                    ' files have been saved sucessfully!\n')

    def __postgresql__delete_from_tables(self):
        """Clear the tables in the PostgreSQL database"""

        self.db_postgres.delete_from_table('collections')
        self.db_postgres.delete_from_table('items')

        logging.info(f'`collections` and `items` tables have been cleared sucessfully!\n')

    ##################################################
    # df_collection and df_item
    ##################################################

    def __configure_df_collection(self):
        logging.info('**************************************************')
        logging.info('*           __configure_df_collection            *')
        logging.info('**************************************************')

        # logging.debug(f'before - df_collection: {self.df_collection} \n')

        # rename column from `ìd` to `name`
        self.df_collection.rename(columns={'id': 'name'}, inplace=True)

        # create `id` column based on the row index value
        self.df_collection["id"] = self.df_collection.index + 1

        # put `id` column as the first column
        self.df_collection = self.df_collection[['id'] + [col for col in self.df_collection.columns if col != 'id']]

        # convert dates from `str` to `date`
        self.df_collection['start_date'] = to_datetime(self.df_collection['start_date']).dt.date
        self.df_collection['end_date'] = to_datetime(self.df_collection['end_date']).dt.date

        # convert bbox from  `str` to `float`
        self.df_collection['min_y'] = self.df_collection['min_y'].astype(float)
        self.df_collection['min_x'] = self.df_collection['min_x'].astype(float)
        self.df_collection['max_y'] = self.df_collection['max_y'].astype(float)
        self.df_collection['max_x'] = self.df_collection['max_x'].astype(float)

        logging.info(f'df_collection: \n{self.df_collection} \n')
        # logging.info(f'df_collection.dtypes: \n{self.df_collection.dtypes} \n')

    def __fix_df_item_columns_order(self):
        # get columns
        columns = self.df_item.columns.tolist()

        # remove 'collection_id' column in the final
        columns.remove('collection_id')

        # add column 'collection_id' in the third position
        columns = columns[:2] + ['collection_id'] + columns[2:]

        # reorder columns
        self.df_item = self.df_item[columns]

    def __configure_df_item(self):
        logging.info('**************************************************')
        logging.info('*              __configure_df_item               *')
        logging.info('**************************************************')

        # logging.debug(f'before - df_item: {self.df_item} \n')

        # rename column from `ìd` to `name`
        self.df_item.rename(columns={'id': 'name'}, inplace=True)

        # create `id` column based on the row index value
        self.df_item["id"] = self.df_item.index + 1

        # put `id` column as the first column
        self.df_item = self.df_item[['id'] + [col for col in self.df_item.columns if col != 'id']]

        # convert dates from `str` to `date`
        self.df_item['datetime'] = to_datetime(self.df_item['datetime'])
        self.df_item['date'] = to_datetime(self.df_item['date']).dt.date

        # convert values from  `str` to `int`
        self.df_item['path'] = self.df_item['path'].astype(int)
        self.df_item['row'] = self.df_item['row'].astype(int)
        self.df_item['cloud_cover'] = self.df_item['cloud_cover'].fillna(0)
        self.df_item['cloud_cover'] = self.df_item['cloud_cover'].astype(int)
        # self.df_item['sync_loss'] = self.df_item['sync_loss'].astype(float)
        self.df_item['deleted'] = self.df_item['deleted'].astype(int)

        # convert bbox from  `str` to `float`
        self.df_item['tl_longitude'] = self.df_item['tl_longitude'].astype(float)
        self.df_item['tl_latitude'] = self.df_item['tl_latitude'].astype(float)
        self.df_item['bl_longitude'] = self.df_item['bl_longitude'].astype(float)
        self.df_item['bl_latitude'] = self.df_item['bl_latitude'].astype(float)
        self.df_item['br_longitude'] = self.df_item['br_longitude'].astype(float)
        self.df_item['br_latitude'] = self.df_item['br_latitude'].astype(float)
        self.df_item['tr_longitude'] = self.df_item['tr_longitude'].astype(float)
        self.df_item['tr_latitude'] = self.df_item['tr_latitude'].astype(float)

        # fix `aseets` column, merge `thumbnail` in `assets`
        self.df_item['assets'] = self.df_item[['thumbnail', 'assets']].apply(fix_assets, axis=1)

        # generate collection_id column
        self.df_item['collection_id'] = self.df_item["collection"].apply(
            lambda collection: generate_collection_id_column(collection, self.df_collection)
        )

        # delete unnecessary columns
        del self.df_item['thumbnail']
        # del self.df_item['collection']

        self.__fix_df_item_columns_order()

        # logging.info(f'df_item: \n{self.df_item.head()} \n\n')
        logging.info(f'df_item: \n{self.df_item[["name", "collection_id", "collection", "assets"]].head()}\n')

    def __configure_dataframes(self):
        # configure dataframes
        self.__configure_df_collection()
        self.__configure_df_item()

        # save a new version of the dataframes after modifications
        self.__save_dfs(
            collection_file_name='collection_configured.csv',
            item_file_name='item_configured.csv'
        )

    ##################################################
    # main
    ##################################################

    def main(self):
        # self.__get_dfs_from_mysqldb()
        # self.__get_dfs_from_csv_files()
        # self.__configure_dataframes()

        self.__get_dfs_from_csv_files(
            collection_file_name='collection_configured.csv',
            item_file_name='item_configured.csv'
        )

        logging.info(f'df_collection: \n{self.df_collection} \n')
        logging.info(f'df_item: \n{self.df_item[["name", "collection_id", "collection", "assets"]].head()}\n')

        # clear tables
        # self.__postgresql__delete_from_tables()


if __name__ == "__main__":
    migrate = MigrateDBs()
    migrate.main()
