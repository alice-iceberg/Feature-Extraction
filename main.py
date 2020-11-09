# This code is used for feature extraction from MindScope project

from psycopg2 import extras as psycopg2_extras
import psycopg2
import pandas as pd

# connection to postgresDB
db_conn = psycopg2.connect(
    host='127.0.0.1',
    database='easytrack_db',
    user='postgres',
    password='nslab123'
)
cur = db_conn.cursor(cursor_factory=psycopg2_extras.DictCursor)

# filename is "[user id]_[datasource_id]"
FILENAME = "16_47.csv"
QUERY = 'select * from data."3-16" where data_source_id=11;'


# convert postgresDB to pandas table
def create_pandas_table(sql_query, database=db_conn):
    table = pd.read_sql_query(sql_query, database)
    return table


def convert_bytea_values(dataframe, from_column, to_column, decoding):
    for key, value in dataframe[from_column].iteritems():
        dataframe[to_column] = bytes(value).decode(decoding)
    return dataframe


table = create_pandas_table(QUERY)
table = convert_bytea_values(table, 'value', 'converted_value', 'utf-8')

# creating new dataframe with only required columns
new_table = pd.DataFrame(table, columns=['timestamp', 'converted_value'])
print(new_table)

# saving new dataframe to csv file
# new_table.to_csv('16-47.csv', index=False, header=True)

cur.close()
db_conn.close()
