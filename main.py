# This code is used for feature extraction from MindScope project

from psycopg2 import extras as psycopg2_extras
import psycopg2
import pandas as pd
import os

# connection to postgresDB
db_conn = psycopg2.connect(
    host='127.0.0.1',
    database='easytrack_db',
    user='postgres',
    password='nslab123'
)
cur = db_conn.cursor(cursor_factory=psycopg2_extras.DictCursor)

USER_ID = 16
CAMPAIGN_ID = 3
DATA_SOURCE_IDs = [38, 39, 40, 41, 19, 42, 22, 43, 44, 45, 46, 1, 2, 28, 47, 29, 48, 10, 49, 11, 50]

# filename is "[user id]_[datasource_id]"
FILENAMES = []

# creating array of filenames
for item, value in enumerate(DATA_SOURCE_IDs):
    _dir = f'{USER_ID}_{value}.csv'
    FILENAMES.append(_dir)


def create_query(campaign_id, user_id, datasource_id):
    query = f'select timestamp, value from data."{campaign_id}-{user_id}" where data_source_id = {datasource_id}'
    return query


# convert postgresDB to pandas table
def create_pandas_table(sql_query, database=db_conn):
    table = pd.read_sql_query(sql_query, database)
    return table


def convert_bytea_values(dataframe, from_column, to_column, decoding):
    for key, value in dataframe[from_column].iteritems():
        dataframe[to_column] = bytes(value).decode(decoding)
    return dataframe


for item, datasource_id in enumerate(DATA_SOURCE_IDs):
    query = create_query(CAMPAIGN_ID, USER_ID, datasource_id)
    table = create_pandas_table(query)
    table = convert_bytea_values(table, 'value', 'converted_value', 'utf-8')

    # creating new dataframe with only required columns
    new_table = pd.DataFrame(table, columns=['timestamp', 'converted_value'])
    print(new_table)

    # saving new dataframe to csv file
    new_table.to_csv(FILENAMES[item], index=False, header=True)

cur.close()
db_conn.close()
