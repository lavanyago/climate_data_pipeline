import json
import data_prep.missing_values as dp
import landing_zone.data_extract as lzde
import raw_zone.data_extract as rzde

datasources=json.load(open('./config/filestore.json'))

if __name__ == '__main__':

    cities_df = lzde.readCsv(datasources['csv']['cities'])
    climate_df = lzde.readCsv(datasources['csv']['climate'])

    # lzde.landing_zone_data_extract()
    # rzde.raw_zone_data_extract()
    # dp.missing_values()



