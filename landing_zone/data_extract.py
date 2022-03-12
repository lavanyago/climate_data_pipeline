import pandas as pd
import logger as log
import logging as log_config
import json
import pyarrow as pa
import pyarrow.parquet as pq


log_location=json.load(open('config/filestore.json'))

def landing_zone_data_extract():
    print(f'you have successfully called landing zone')

def readCsv(file):
    log_config.basicConfig(filename=log_location['logger']['landing_zone_logger'],
                           level=log_config.INFO,
                           format='%(asctime)s %(levelname)-8s %(message)s',
                           datefmt='%Y-%m-%d %H:%M:%S'
                            )
    df = pd.read_csv(file)
    log.write (f'{file} has missing values as listed below with shape {df.shape}:')
    log.write (df.isna().sum())
    return df

def write_to_landing_zone(df,df_name):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, log_location['landing'][df_name])
    log.write(f'{df_name} added to landing zone with shape {df.shape}')


