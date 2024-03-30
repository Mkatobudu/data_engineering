#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from datetime import time
from time import time
import argparse
import os


def main(params):
    # user
    # password
    # host
    # port
    # db
    # table_name
    # url of the csv
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")

    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    engine = create_engine(postgres_url)
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    df.head(n=0).to_sql(table_name, con=engine, if_exists="replace", index=False)

    while True:
        t_start = time()

        try:
            df = next(df_iter)
            
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
   
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False)

        except Exception as e:
            print(f"Insert did not complete successfully and the error is: {e}")

        t_end = time()

        print("Inserted another chunk..., took %.3f seconds" % (t_end - t_start))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest csv data to PostgreSQL.')

    parser.add_argument('user', help='username for PostgreSQL')
    parser.add_argument('password', help='password for PostgreSQL')
    parser.add_argument('host', help='host for PostgreSQL')
    parser.add_argument('port', help='port for PostgreSQL')
    parser.add_argument('db', help='database name for PostgreSQL')
    parser.add_argument('table_name', help='name of our table in PostgreSQL')
    parser.add_argument('url', help='url for the csv data')

    args = parser.parse_args()

    main(args)
