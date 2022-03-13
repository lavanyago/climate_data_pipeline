import json
import data_prep.missing_values as dp
import landing_zone.data_extract as lzde
import raw_zone.data_extract as rzde


datasources = json.load(open('./config/filestore.json'))

if __name__ == '__main__':
    # source data from sources
    cities_df = lzde.readCsv(datasources['csv']['cities'])
    climate_df = lzde.readCsv(datasources['csv']['climate'])
    # write to landing  zone
    lzde.write_to_landing_zone(cities_df, 'cities')
    lzde.write_to_landing_zone(climate_df, 'climate')

    # # read from landing zone & write to raw zone
    cities_df = rzde.readParquet(datasources['landing']['cities'])
    climate_df = rzde.readParquet(datasources['landing']['climate'])

    specific_columns = ['lng', 'lat', 'STATION_NAME', 'CLIMATE_IDENTIFIER', 'ID', 'LOCAL_DATE', 'PROVINCE_CODE', 'LOCAL_YEAR',
                        'LOCAL_MONTH', 'LOCAL_DAY', 'MEAN_TEMPERATURE', 'MEAN_TEMPERATURE_FLAG']

    climate_df_without_na = dp.fill_missing_values(climate_df[specific_columns])

    specific_columns =     specific_columns = ['lng', 'lat', 'STATION_NAME', 'CLIMATE_IDENTIFIER', 'ID', 'LOCAL_DATE', 'PROVINCE_CODE', 'LOCAL_YEAR',
                        'LOCAL_MONTH', 'LOCAL_DAY', 'MEAN_TEMPERATURE_FILLED']

    rzde.write_to_raw_zone(climate_df_without_na[specific_columns],'climate')
    rzde.write_to_raw_zone(cities_df,'cities')




    # lzde.landing_zone_data_extract()
    # rzde.raw_zone_data_extract()
    # dp.missing_values()
