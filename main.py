#!/usr/bin/env python
# -*- coding: utf-8 -*-

from json import dumps, loads
from pandas import read_csv, to_datetime

from modules.environment import DATA_PATH, DATA_FIXED_PATH
from modules.logging import logging
from modules.model import MySQLConnection, PostgreSQLConnection
from modules.utils import delete_and_recreate_folder


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


def generate_insert_clause_column(row):
    srid = 4326
    min_x = row["bl_longitude"]
    min_y = row["bl_latitude"]
    max_x = row["tr_longitude"]
    max_y = row["tr_latitude"]

    metadata = {
        # 'datetime': row["datetime"],
        # 'date': row["date"],
        'path': row["path"],
        'row': row["row"],
        'satellite': row["satellite"],
        'sensor': row["sensor"],
        # 'cloud_cover': row["cloud_cover"],
        'sync_loss': row["sync_loss"],
        'deleted': row["deleted"]
    }

    return (
        'INSERT INTO bdc.items '
        '(id, name, collection_id, start_date, end_date, '
        'cloud_cover, assets, metadata, geom, min_convex_hull, srid) '
        'VALUES '
        f'({row["id"]}, \'{row["name"]}\', {row["collection_id"]}, \'{row["datetime"]}\', \'{row["datetime"]}\', '
        f'{row["cloud_cover"]}, \'{row["assets"]}\', \'{dumps(metadata)}\', '
        f'ST_MakeEnvelope({min_x}, {min_y}, {max_x}, {max_y}, {srid}), '
        f'ST_MakeEnvelope({min_x}, {min_y}, {max_x}, {max_y}, {srid}), {srid});'
    )


class MigrateDBs():

    def __init__(self):
        # create PostgreSQL connection
        self.db_postgres = PostgreSQLConnection()

    ##################################################
    # get the dataframes
    ##################################################

    def __get_dfs_from_mysqldb(self):
        # MySQL connection
        db_mysql = MySQLConnection()

        # get the dfs from database
        self.df_collection = db_mysql.select_from_collection()
        self.df_item = db_mysql.select_from_item()

    def __get_dfs_from_csv_files(self, collection_file_name='collection.csv',
                                                           item_file_name='item.csv',
                                                           resolution_unit_file_name='resolution_unit.csv',
                                                           band_file_name='band.csv'):
        # get the dfs from CSV files
        self.df_collection = read_csv(DATA_PATH + collection_file_name)
        self.df_item = read_csv(DATA_PATH + item_file_name)

        self.df_resolution_unit = read_csv(DATA_FIXED_PATH + resolution_unit_file_name)
        self.df_band = read_csv(DATA_FIXED_PATH + band_file_name)

    ##################################################
    # other
    ##################################################

    def __save_dfs(self, collection_file_name='collection.csv', item_file_name='item.csv'):
        """Save the dataframes in CSV files"""

        logging.info('**************************************************')
        logging.info('*                   __save_dfs                   *')
        logging.info('**************************************************')

        self.df_collection.to_csv(DATA_PATH + collection_file_name, index=False)
        self.df_item.to_csv(DATA_PATH + item_file_name, index=False)

        logging.info(f'`{collection_file_name}` and `{item_file_name}` '
                      'files have been saved sucessfully!\n')

    def __delete_from_tables(self):
        """Clear the tables in the PostgreSQL database"""

        logging.info('**************************************************')
        logging.info('*              __delete_from_tables              *')
        logging.info('**************************************************')

        self.db_postgres.delete_from_table('bdc.collections')
        self.db_postgres.delete_from_table('bdc.items')

        logging.info(f'`collections` and `items` tables have been cleared sucessfully!\n')

    ##################################################
    # df_resolution_unit and df_band
    ##################################################

    def __configure_dfs_resolution_and_band(self):
        logging.info('**************************************************')
        logging.info('*       __configure_dfs_resolution_and_band      *')
        logging.info('**************************************************')

        # create `id` column based on the row index value
        self.df_resolution_unit["id"] = self.df_resolution_unit.index + 1

        # put `id` column as the first column
        self.df_resolution_unit = self.df_resolution_unit[['id'] + [col for col in self.df_resolution_unit.columns if col != 'id']]

        logging.info(f'df_resolution_unit: \n{self.df_resolution_unit} \n')
        logging.info(f'df_band: \n{self.df_band} \n')

    ##################################################
    # df_collection
    ##################################################

    def __configure_df_collection__fix_columns_types(self):
        # convert dates from `str` to `date`
        self.df_collection['start_date'] = to_datetime(self.df_collection['start_date']).dt.date
        self.df_collection['end_date'] = to_datetime(self.df_collection['end_date']).dt.date

        # convert bbox from  `str` to `float`
        self.df_collection['min_y'] = self.df_collection['min_y'].astype(float)
        self.df_collection['min_x'] = self.df_collection['min_x'].astype(float)
        self.df_collection['max_y'] = self.df_collection['max_y'].astype(float)
        self.df_collection['max_x'] = self.df_collection['max_x'].astype(float)

    def __configure_df_collection(self):
        logging.info('**************************************************')
        logging.info('*           __configure_df_collection            *')
        logging.info('**************************************************')

        # rename column from `ìd` to `name`
        self.df_collection.rename(columns={'id': 'name'}, inplace=True)

        # create `id` column based on the row index value
        self.df_collection["id"] = self.df_collection.index + 1

        # put `id` column as the first column
        self.df_collection = self.df_collection[['id'] + [col for col in self.df_collection.columns if col != 'id']]

        self.__configure_df_collection__fix_columns_types()

        logging.info(f'df_collection: \n{self.df_collection} \n')

    def __insert_df_collection_into_database(self):
        logging.info('**************************************************')
        logging.info('*      __insert_df_collection_into_database      *')
        logging.info('**************************************************')

        for collection in self.df_collection.itertuples():
            logging.info(f'Inserting `{collection.name}` collection in the database...')
            self.db_postgres.insert_into_collections(**collection._asdict())
        logging.info(f'All collections have been inserted in the database sucessfully!\n')

    ##################################################
    # df_item
    ##################################################

    def __configure_df_item__fix_columns_types(self):
        # convert dates from `str` to `date`
        self.df_item['datetime'] = to_datetime(self.df_item['datetime'])
        self.df_item['date'] = to_datetime(self.df_item['date']).dt.date

        # convert values from  `str` to `int`
        self.df_item['path'] = self.df_item['path'].astype(int)
        self.df_item['row'] = self.df_item['row'].astype(int)
        self.df_item['cloud_cover'] = self.df_item['cloud_cover'].fillna(0)
        self.df_item['cloud_cover'] = self.df_item['cloud_cover'].astype(int)
        self.df_item['sync_loss'] = self.df_item['sync_loss'].fillna(0)
        self.df_item['sync_loss'] = self.df_item['sync_loss'].astype(float)
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

        # rename column from `ìd` to `name`
        self.df_item.rename(columns={'id': 'name'}, inplace=True)

        # create `id` column based on the row index value
        self.df_item["id"] = self.df_item.index + 1

        # put `id` column as the first column
        self.df_item = self.df_item[['id'] + [col for col in self.df_item.columns if col != 'id']]

        self.__configure_df_item__fix_columns_types()

        self.df_item['thumbnail'] = self.df_item['thumbnail'].fillna('')

        # fix `aseets` column, merge `thumbnail` in `assets`
        self.df_item['assets'] = self.df_item[['thumbnail', 'assets']].apply(fix_assets, axis=1)

        # generate collection_id column
        self.df_item['collection_id'] = self.df_item["collection"].apply(
            lambda collection: generate_collection_id_column(collection, self.df_collection)
        )

        # generate INSERT clause for each row
        self.df_item['insert'] = self.df_item.apply(generate_insert_clause_column, axis=1)

        # delete unnecessary columns
        del self.df_item['thumbnail']
        # del self.df_item['collection']

        self.__fix_df_item_columns_order()

        # logging.info(f'df_item: \n{self.df_item.head()} \n\n')
        logging.info(f'df_item: \n{self.df_item[["name", "collection_id", "collection", "assets"]].head()}\n')

    def __insert_df_item_into_database(self):
        logging.info('**************************************************')
        logging.info('*         __insert_df_item_into_database         *')
        logging.info('**************************************************')

        size_df_item = len(self.df_item)

        logging.info(f'size_df_item: {size_df_item}')

        # fill `items` table by chunks
        step = 10000
        for start_slice in range(0, size_df_item, step):
            end_slice = start_slice + step
            if end_slice > size_df_item:
                end_slice = size_df_item

            # concatenate the INSERT clauses to execute many statements in one time
            insert_clauses = ' '.join(self.df_item[start_slice:end_slice]['insert'].tolist())

            logging.info(f'Inserting items[{start_slice}, {end_slice}] in the database...')
            self.db_postgres.execute(insert_clauses, is_transaction=True)

        logging.info(f'All items have been inserted in the database sucessfully!\n')

    ##################################################
    # main
    ##################################################

    def __main__get_dfs_configure_dfs_and_save_dfs(self, is_to_get_dfs_from_db=True):
        logging.info('**************************************************')
        logging.info('*                 main - settings                *')
        logging.info('**************************************************')

        if is_to_get_dfs_from_db:
            # delete and recreate `assets` folder
            delete_and_recreate_folder(DATA_PATH)
            logging.info(f'`{DATA_PATH}` folder has been recreated sucessfully!\n')

            # get dataframes from database and save them in CSV files
            self.__get_dfs_from_mysqldb()
            self.__save_dfs()

        # get the saved dataframes
        self.__get_dfs_from_csv_files()

        # configure dataframes
        self.__configure_dfs_resolution_and_band()
        self.__configure_df_collection()
        self.__configure_df_item()

        # save a new version of the dataframes after modifications
        self.__save_dfs(
            collection_file_name='collection_configured.csv',
            item_file_name='item_configured.csv'
        )

    def main(self):
        self.__main__get_dfs_configure_dfs_and_save_dfs(is_to_get_dfs_from_db=True)

        self.__get_dfs_from_csv_files(
            collection_file_name='collection_configured.csv',
            item_file_name='item_configured.csv'
        )

        self.__configure_df_collection__fix_columns_types()
        self.__configure_df_item__fix_columns_types()

        logging.info('**************************************************')
        logging.info('*                      main                      *')
        logging.info('**************************************************')

        logging.info(f'df_collection: \n{self.df_collection} \n')
        logging.info(f'df_item: \n{self.df_item[["name", "collection_id", "collection", "insert"]].head()}\n')

        self.__delete_from_tables()
        self.__insert_df_collection_into_database()
        self.__insert_df_item_into_database()


if __name__ == "__main__":
    migrate = MigrateDBs()
    migrate.main()
