import pandas as pd


def fill_missing_values(df):
    #avg of back fill and forward fill value
    df = df.assign(MEAN_TEMPERATURE_FILLED=pd.Series(pd.concat([df['MEAN_TEMPERATURE'].ffill(),
                                                                df['MEAN_TEMPERATURE'].bfill()]).groupby(level=0).mean()).values)
    return(df)