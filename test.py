import datetime
from datetime import datetime, timedelta
import time
import pandas as pd

def get_brizo_data(row, now=datetime.utcnow(), yesterday=datetime.utcnow() - timedelta(days=1)):
    now_formatted = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    yesterday_formatted = yesterday.strftime('%Y-%m-%dT%H:%M:%SZ')
    start_date = yesterday_formatted
    end_date = now_formatted

    brizo_code = instruments_df.loc[row, 'Brizo']
    brizo_name = instruments_df.loc[row, 'Brizo']
    api_key = instruments_df.loc[row, 'API Key']
    id_brizo = instruments_df.loc[row, 'id_brizo']
    b_name = brizo_name.lower().replace("_", "")

    CSV_URL = (f'https://api.thingspeak.com/channels/{id_brizo}/feeds.csv?start={start_date}&end={end_date}&api_key={api_key}')
    print(CSV_URL)

instruments_data = {
    'Brizo': ['Brizo_1', 'Brizo_2', 'Brizo_3', 'Brizo_4', 'Brizo_5'],
    'API Key': ['5CJF0937J96I3LV1',  'SU83XM2R70RPZLYS', 'WJFABTA77UAE2SKG', '3UZZUCLRBLYDELXV', '1KTSMKIFO5EC8N1E'],
    'id_brizo': [1433077, 1652965, 1652966, 1653050, 1783137]
}

instruments_df = pd.DataFrame(instruments_data)

def correction_daily():
    start = datetime(2020, 1, 1)
    end = datetime(2024, 1, 1)
    timestamps = [start]
    ts = start
    while ts < end:
        ts += timedelta(days=1)
        timestamps.append(ts)
    for day in timestamps:
        day_before = day - timedelta(days=1)
        for  row in range(len(instruments_df. index)): 
            try:
                get_brizo_data(row, now=day, yesterday=day_before)
            except Exception as e:
                continue

correction_daily()
      