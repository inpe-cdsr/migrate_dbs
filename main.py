#!/usr/bin/env python
# -*- coding: utf-8 -*-

from json import dumps, loads
from pandas import read_csv, to_datetime

from modules.logging import logging
from modules.model import DatabaseConnection


def fix_assets(assets_string):
    """Fix column `assets`"""

    new_assets = {}

    # convert from `str` to `dict` and iterate over it
    for asset in loads(assets_string):
        new_assets[asset['band']] = {
            'href': asset['href'],
            'type': 'image/tiff; application=geotiff',
        }
        new_assets[asset['band'] + '_xml'] = {
            'href': asset['href'].replace('.tif', '.xml'),
            'type': 'application/xml'
        }

    # logging.debug(f'new_assets: \n{new_assets}\n')

    # convert from `dict` to `str`
    return dumps(new_assets)


class MigrateDBs():

    def __init__(self):
        # self.__get_dfs_from_db()
        self.__get_dfs_from_csv_files()

    ##################################################
    # get the dataframes
    ##################################################

    def __get_dfs_from_db(self):
        # database connection
        self.db = DatabaseConnection()

        # get the dfs from database
        self.df_collection = self.db.select_from_collection()
        self.df_item = self.db.select_from_item()

        # save the dataframes in CSV files
        self.__save_dfs()

    def __get_dfs_from_csv_files(self):
        # get the dfs from CSV files
        self.df_collection = read_csv('data/collection.csv')
        self.df_item = read_csv('data/item.csv')

    ##################################################
    # save the dataframes in CSV files
    ##################################################

    def __save_dfs(self):
        self.df_collection.to_csv('data/collection.csv', index=False)
        self.df_item.to_csv('data/item.csv', index=False)

        logging.info('Dataframes have been saved sucessfully.\n')

    ##################################################
    # df_collection
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

    ##################################################
    # df_item
    ##################################################

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

        # fix `aseets` column
        self.df_item['assets'] = self.df_item['assets'].apply(fix_assets)

        # logging.info(f'df_item: \n{self.df_item.head()} \n\n')
        logging.info(f'df_item: \n{self.df_item[["name", "assets"]].head(10)}\n')

    ##################################################
    # main
    ##################################################

    def main(self):
        self.__configure_df_collection()
        self.__configure_df_item()


if __name__ == "__main__":
    migrate = MigrateDBs()
    migrate.main()
