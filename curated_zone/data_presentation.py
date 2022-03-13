import pandas as pd
import logger as log
import numpy as np
import sklearn.neighbors

def readParquet(file):
    df = pd.read_parquet(file)
    log.write(f'on reading {file} has missing values as listed below with shape {df.shape}:')
    log.write(df.isna().sum())
    return df


def merge_climate_cities_datasets(climate,cities):
    # Insert new columns with radians for lat-lon to dataframes using np.radians
    climate[['lat_radians_climate', 'long_radians_climate']] = (
        np.radians(climate.loc[:, ['lat', 'lng']])
    )
    cities[['lat_radians_cities', 'long_radians_cities']] = (
        np.radians(cities.loc[:, ['lat', 'lng']])
    )

    dist = sklearn.metrics.DistanceMetric.get_metric('haversine')
    dist_matrix = (dist.pairwise
                   (climate[['lat_radians_climate', 'long_radians_climate']],
                    cities[['lat_radians_cities', 'long_radians_cities']]) * 3959
                   )
    # Note that 3959 is the radius of the earth in miles

    df_dist_matrix = (
        pd.DataFrame(dist_matrix,index=climate['CLIMATE_IDENTIFIER'],
                     columns=cities['city'])
    )

    df_dist_unpv = (
        pd.melt(df_dist_matrix.reset_index(), id_vars='CLIMATE_IDENTIFIER')
    )
    # Rename this column to 'distance' for relevance.
    df_dist_unpv = df_dist_unpv.rename(columns={'value': 'DISTANCE'})

    #retreive closest City for each observatory
    df_dist_unpv['MIN_DISTANCE']=df_dist_unpv.groupby(['CLIMATE_IDENTIFIER'])['DISTANCE'].transform('min')

    return df_dist_unpv[df_dist_unpv['MIN_DISTANCE'] == df_dist_unpv['DISTANCE']].iloc[:,0:3]

def city_wise_mean_median(df):
    df['CITY_MEAN'] = df.groupby(['city'])['MEAN_TEMPERATURE_FILLED'].transform('mean')
    df['CITY_MEDIAN'] = df.groupby(['city'])['MEAN_TEMPERATURE_FILLED'].transform('median')
    selected_columns=['city','LOCAL_DATE','CITY_MEAN','CITY_MEDIAN']
    return df[selected_columns].drop_duplicates()
