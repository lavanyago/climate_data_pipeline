import pandas as pd
import json
import logger as log
import snappy
import fastparquet

log_location=json.load(open('config/filestore.json'))

def raw_zone_data_extract():
    print(f'you have successfully called raw zone')

def readParquet(file):
    df = pd.read_parquet(file)
    log.write(f'on reading {file} has missing values as listed below with shape {df.shape}:')
    log.write(df.isna().sum())
    return df

def write_to_raw_zone(df,df_name):
    df.to_parquet(log_location['raw'][df_name], compression='snappy')
    log.write( f'{df_name} added to raw zone with shape {df.shape} in parquet format')