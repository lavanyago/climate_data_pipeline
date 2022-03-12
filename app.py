import data_prep.missing_values as dp
import landing_zone.data_extract as lzde
import raw_zone.data_extract as rzde

if __name__ == '__main__':
    lzde.landing_zone_data_extract()
    rzde.raw_zone_data_extract()
    dp.missing_values()



