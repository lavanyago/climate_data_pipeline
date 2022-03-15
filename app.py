import pandas as pd
import json
import data_prep.missing_values as dp
import landing_zone.data_extract as lzde
import raw_zone.data_extract as rzde
import curated_zone.data_presentation as cdp
import sys

datasources = json.load(open('./config/filestore.json'))

def app(date):
    # source data from sources
    cities_df = lzde.readCsv(datasources['csv']['cities'])
    climate_df = lzde.readCsv(datasources['csv']['climate'])
    # write to landing  zone
    # climate_df['LOCAL_DATE']=pd.to_datetime(climate_df['LOCAL_DATE']).dt.date

    lzde.write_to_landing_zone(cities_df, 'cities')
    lzde.write_to_landing_zone(climate_df, 'climate')

    # read from landing zone & write to raw zone
    cities_df = rzde.readParquet(datasources['landing']['cities'])
    climate_df = rzde.readParquet(datasources['landing']['climate'])

    climate_df['LOCAL_DATE'] = pd.to_datetime(climate_df['LOCAL_DATE'])
    specific_columns = ['lng', 'lat', 'STATION_NAME', 'CLIMATE_IDENTIFIER', 'ID', 'LOCAL_DATE', 'PROVINCE_CODE', 'LOCAL_YEAR',
                        'LOCAL_MONTH', 'LOCAL_DAY', 'MEAN_TEMPERATURE', 'MEAN_TEMPERATURE_FLAG']

    climate_df_without_na = dp.fill_missing_values(climate_df[specific_columns])

    specific_columns = ['lng', 'lat', 'STATION_NAME', 'CLIMATE_IDENTIFIER', 'ID', 'LOCAL_DATE', 'PROVINCE_CODE', 'LOCAL_YEAR',
                        'LOCAL_MONTH', 'LOCAL_DAY', 'MEAN_TEMPERATURE', 'MEAN_TEMPERATURE_FLAG', 'MEAN_TEMPERATURE_FILLED']

    rzde.write_to_raw_zone(climate_df_without_na[specific_columns],'climate')
    rzde.write_to_raw_zone(cities_df,'cities')

    # write summary to curated zone
    cities_df_raw = cdp.readParquet(datasources['raw']['cities'])
    climate_df_raw = cdp.readParquet(datasources['raw']['climate'])

    observatory_city_mapper = cdp.merge_climate_cities_datasets(climate_df_raw[['lng','lat','CLIMATE_IDENTIFIER']].drop_duplicates(),
                                      cities_df_raw)

    #add City to climate_df_raw
    observatory_city_df = climate_df_raw.merge(observatory_city_mapper, on=['CLIMATE_IDENTIFIER'],how="left")
    observatory_city_df = observatory_city_df.merge(cities_df_raw, on = ['city'], how="left")

    #get Mean & Median by City
    cdp.city_wise_mean_median(observatory_city_df[(observatory_city_df['LOCAL_DATE']==date)]
            ).to_csv(datasources['curated']['climate_analysis'],index=False)


if __name__ == '__main__':
    app(sys.argv[1])