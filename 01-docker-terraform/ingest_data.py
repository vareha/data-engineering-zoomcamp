#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from time import time
import os
import argparse

def main(params):
    user = 'root'
    password = 'root'
    host = 'localhost'
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    if url.endswith('.gz'):
        os.system(f'curl -L "{url}" | gunzip > {csv_name}')
    else:
        os.system(f'curl -L "{url}" > {csv_name}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)
    df = next(df_iter)

    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    while True:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Database Arguments')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--url', help='url of the csv file')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')

    args = parser.parse_args()
    main(args)