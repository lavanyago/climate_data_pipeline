import pandas as pd
import logger as log
import logging as log_config
import json

log_location=json.load(open('config/filestore.json'))

def landing_zone_data_extract():
    print(f'you have successfully called landing zone')

def readCsv(file):
    log_config.basicConfig(filename=log_location['logger']['landing_zone_logger'], level=log_config.INFO)
    df = pd.read_csv(file)
    log.write (f'{file} has missing values as listed below:')
    log.write (df.isna().sum())
    return df

