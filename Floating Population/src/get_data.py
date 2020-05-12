import numpy as np
import pandas as pd
import time
from datetime import datetime

consumerKey = 'hnuzMJ1QGE3TcjlYiXohfA6vkGb55OT9xKtTCcd31L4'

citymaster_url = "https://api-tokyochallenge.odpt.org/api/v4/files/Agoop/data/prefcode_citycode_master_UTF-8.csv?acl:consumerKey={}"
df_citycode = pd.read_csv(citymaster_url.format(consumerKey))

tokyo_32_dict = dict(df_citycode[df_citycode.prefname=='東京都'].loc[:,['cityname', 'citycode']].values[:32])

def count_id_by_city(df):
    """
    Counts the unique daily id within each city
    """
    return (df.set_index(['citycode'])
            .loc[tokyo_32_dict.values(),['dailyid']]
            .reset_index()
            .groupby(['citycode'])
            .nunique(['dailyid'])
            .drop(['citycode'], axis=1)
            .reset_index()
           )

def append_timestamp(df, date):
    df['datetime']  = date
    return df

def get_data(start: str, days: int):
    """
    start: starting date in 'yyyy-mm-dd' format
    days: Number of days of data we want to obtain 
    """
    url = 'https://api-tokyochallenge.odpt.org/api/v4/files/Agoop/data/PDP_{}.csv?acl:consumerKey={}'
    consumerKey = 'hnuzMJ1QGE3TcjlYiXohfA6vkGb55OT9xKtTCcd31L4'
    
    periods = days * 24
    df_list = []
    for date in pd.date_range(start=start, periods=periods, freq='H').tolist():
        date_str = datetime.strftime(date, "%Y%m%d_%H")
        try:
            df = (pd.read_csv(url.format(date_str, consumerKey))
                  .pipe(count_id_by_city)
                  .pipe(append_timestamp, date)
                 )
            df_list.append(df)
            
            print('getting data from: {}'.format(date))
            
            time.sleep(1)
        except:
            continue
            
    return pd.concat(df_list)

df = get_data('2019-06-01', 37)

df.to_csv('floating_population')