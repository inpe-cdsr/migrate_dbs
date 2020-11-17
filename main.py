#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandas import read_csv, to_datetime

from modules.logging import logging
from modules.model import DatabaseConnection


##################################################
# get the dataframes
##################################################

def get_dfs_from_db():
    # database connection
    db = DatabaseConnection()

    # get the dfs from database
    df_collection = db.select_from_stac_collection()
    df_item = db.select_from_stac_item()

    return df_collection, df_item


def get_dfs_from_csv_files():
    # get the dfs from CSV files
    df_collection = read_csv('data/collection.csv')
    df_item = read_csv('data/item.csv')

    return df_collection, df_item


# df_collection, df_item = get_dfs_from_db()
df_collection, df_item = get_dfs_from_csv_files()


##################################################
# save the dfs to csv files
##################################################

# df_collection.to_csv('data/collection.csv', index=False)
# df_item.to_csv('data/item.csv', index=False)

# logging.info('Dataframes have been saved sucessfully.\n')


##################################################
# df_collection
##################################################

logging.info('**************************************************')
logging.info('*                 df_collection                  *')
logging.info('**************************************************')

# logging.info(f'before - df_collection: {df_collection} \n')

# rename column
df_collection.rename(columns={'id': 'name'}, inplace=True)

# create `id` column based on the index row value
df_collection["id"] = df_collection.index + 1

# put `id` column as the first column
df_collection = df_collection[ ['id'] + [ col for col in df_collection.columns if col != 'id' ] ]

df_collection['start_date'] = to_datetime(df_collection['start_date']).dt.date
df_collection['end_date'] = to_datetime(df_collection['end_date']).dt.date

df_collection['min_y'] = df_collection['min_y'].astype(float)
df_collection['min_x'] = df_collection['min_x'].astype(float)
df_collection['max_y'] = df_collection['max_y'].astype(float)
df_collection['max_x'] = df_collection['max_x'].astype(float)

logging.info(f'df_collection: \n{df_collection} \n\n')


##################################################
# df_item
##################################################

logging.info('**************************************************')
logging.info('*                    df_item                     *')
logging.info('**************************************************')

# logging.info(f'before - df_item: {df_item} \n')

# rename column
df_item.rename(columns={'id': 'name'}, inplace=True)

# create `id` column based on the index row value
df_item["id"] = df_item.index + 1

# put `id` column as the first column
df_item = df_item[ ['id'] + [ col for col in df_item.columns if col != 'id' ] ]

df_item['datetime'] = to_datetime(df_item['datetime'])
df_item['date'] = to_datetime(df_item['date']).dt.date

df_item['path'] = df_item['path'].astype(int)
df_item['row'] = df_item['row'].astype(int)
df_item['cloud_cover'] = df_item['cloud_cover'].fillna(0)
df_item['cloud_cover'] = df_item['cloud_cover'].astype(int)
# df_item['sync_loss'] = df_item['sync_loss'].astype(float)
df_item['deleted'] = df_item['deleted'].astype(int)

df_item['tl_longitude'] = df_item['tl_longitude'].astype(float)
df_item['tl_latitude'] = df_item['tl_latitude'].astype(float)
df_item['bl_longitude'] = df_item['bl_longitude'].astype(float)
df_item['bl_latitude'] = df_item['bl_latitude'].astype(float)
df_item['br_longitude'] = df_item['br_longitude'].astype(float)
df_item['br_latitude'] = df_item['br_latitude'].astype(float)
df_item['tr_longitude'] = df_item['tr_longitude'].astype(float)
df_item['tr_latitude'] = df_item['tr_latitude'].astype(float)

logging.info(f'df_item: \n{df_item.head()} \n\n')
