#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandas import read_csv, to_datetime

from modules.logging import logging
from modules.model import DatabaseConnection


class MigrateDBs():

    def __init__(self):
        # self.df_collection, self.df_item = self.__get_dfs_from_db()
        self.df_collection, self.df_item = self.__get_dfs_from_csv_files()
        # self.__save_dfs()

    ##################################################
    # get the dataframes
    ##################################################

    def __get_dfs_from_db(self):
        # database connection
        db = DatabaseConnection()

        # get the dfs from database
        df_collection = db.select_from_stac_collection()
        df_item = db.select_from_stac_item()

        return df_collection, df_item

    def __get_dfs_from_csv_files(self):
        # get the dfs from CSV files
        df_collection = read_csv('data/collection.csv')
        df_item = read_csv('data/item.csv')

        return df_collection, df_item

    ##################################################
    # save the dfs in to csv files
    ##################################################

    def __save_dfs():
        self.df_collection.to_csv('data/collection.csv', index=False)
        self.df_item.to_csv('data/item.csv', index=False)

        logging.info('Dataframes have been saved sucessfully.\n')

    ##################################################
    # df_collection
    ##################################################

    def __configure_df_collection(self):
        logging.info('**************************************************')
        logging.info('*                 df_collection                  *')
        logging.info('**************************************************')

        # logging.info(f'before - df_collection: {self.df_collection} \n')

        # rename column
        self.df_collection.rename(columns={'id': 'name'}, inplace=True)

        # create `id` column based on the index row value
        self.df_collection["id"] = self.df_collection.index + 1

        # put `id` column as the first column
        self.df_collection = self.df_collection[ ['id'] + [ col for col in self.df_collection.columns if col != 'id' ] ]

        self.df_collection['start_date'] = to_datetime(self.df_collection['start_date']).dt.date
        self.df_collection['end_date'] = to_datetime(self.df_collection['end_date']).dt.date

        self.df_collection['min_y'] = self.df_collection['min_y'].astype(float)
        self.df_collection['min_x'] = self.df_collection['min_x'].astype(float)
        self.df_collection['max_y'] = self.df_collection['max_y'].astype(float)
        self.df_collection['max_x'] = self.df_collection['max_x'].astype(float)

        logging.info(f'df_collection: \n{self.df_collection} \n\n')

    ##################################################
    # df_item
    ##################################################

    def __configure_df_item(self):
        logging.info('**************************************************')
        logging.info('*                    df_item                     *')
        logging.info('**************************************************')

        # logging.info(f'before - df_item: {self.df_item} \n')

        # rename column
        self.df_item.rename(columns={'id': 'name'}, inplace=True)

        # create `id` column based on the index row value
        self.df_item["id"] = self.df_item.index + 1

        # put `id` column as the first column
        self.df_item = self.df_item[ ['id'] + [ col for col in self.df_item.columns if col != 'id' ] ]

        self.df_item['datetime'] = to_datetime(self.df_item['datetime'])
        self.df_item['date'] = to_datetime(self.df_item['date']).dt.date

        self.df_item['path'] = self.df_item['path'].astype(int)
        self.df_item['row'] = self.df_item['row'].astype(int)
        self.df_item['cloud_cover'] = self.df_item['cloud_cover'].fillna(0)
        self.df_item['cloud_cover'] = self.df_item['cloud_cover'].astype(int)
        # self.df_item['sync_loss'] = self.df_item['sync_loss'].astype(float)
        self.df_item['deleted'] = self.df_item['deleted'].astype(int)

        self.df_item['tl_longitude'] = self.df_item['tl_longitude'].astype(float)
        self.df_item['tl_latitude'] = self.df_item['tl_latitude'].astype(float)
        self.df_item['bl_longitude'] = self.df_item['bl_longitude'].astype(float)
        self.df_item['bl_latitude'] = self.df_item['bl_latitude'].astype(float)
        self.df_item['br_longitude'] = self.df_item['br_longitude'].astype(float)
        self.df_item['br_latitude'] = self.df_item['br_latitude'].astype(float)
        self.df_item['tr_longitude'] = self.df_item['tr_longitude'].astype(float)
        self.df_item['tr_latitude'] = self.df_item['tr_latitude'].astype(float)

        logging.info(f'df_item: \n{self.df_item.head()} \n\n')

    ##################################################
    # main
    ##################################################

    def main(self):
        self.__configure_df_collection()
        self.__configure_df_item()


if __name__ == "__main__":
    migrate = MigrateDBs()
    migrate.main()
