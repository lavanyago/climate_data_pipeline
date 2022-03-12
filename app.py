import json
import data_prep.missing_values as dp
import landing_zone.data_extract as lzde
import raw_zone.data_extract as rzde

datasources=json.load(open('./config/filestore.json'))

if __name__ == '__main__':
    # source data from sources
    cities_df = lzde.readCsv(datasources['csv']['cities'])
    climate_df = lzde.readCsv(datasources['csv']['climate'])
    # write to landing  zone
    lzde.write_to_landing_zone(cities_df,'cities')
    lzde.write_to_landing_zone(climate_df,'climate')

    # source data from
    # lzde.landing_zone_data_extract()
    # rzde.raw_zone_data_extract()
    # dp.missing_values()



