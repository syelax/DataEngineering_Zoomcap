#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
import pyarrow.parquet as pq
import os
from sqlalchemy import create_engine
from time import time



def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    file_name = 'output.parquet'
    os.system(f"wget {url} -O {file_name}")

    # create connection to postgres
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df = pq.ParquetFile(file_name)


    # check to replace table
    first_batch_check = 0

    # write 100,000 of data to the database at a time
    for chunk in df.iter_batches(100_000):
        t_start = time() # start time for sanity check
        t_df = chunk.to_pandas()
        # if this is the first time chunk of the data it will replace the table if the data exists 
        if first_batch_check < 1:
            t_df.to_sql(name=table_name, con=engine, if_exists='replace')
            first_batch_check = 1 # change  first batch check to break logic on the next iteration
            # end time for sanity check
            t_end = time()
            print('Inserted first chunk, took %.3f second' % (t_end - t_start))
        else:
            # append rows to table
            t_df.to_sql(name=table_name, con=engine, if_exists='append')
            # end time for sanity check
            t_end = time()
            print('Inserted another chunk, took %.3f second' % (t_end - t_start))

    zone = pd.read_csv('taxi_zone.csv')
    zone.to_sql(name='zones', con=engine, if_exists='replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument('--user', help='username for the postgresql database')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres', type=int )
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table that we will write the results to')
    parser.add_argument('--url', help='url to parquet file')


    args = parser.parse_args()
    
    main(args)