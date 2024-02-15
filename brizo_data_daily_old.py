import pandas as pd
import requests
import os
import csv
import io
import numpy as np
from datetime import date
from datetime import datetime
from datetime import timedelta

def send_to_erddap(csv_url):

  df = pd.read_csv(csv_url)
  df = df.astype(str)
  burl= 'https://data-nautilos-h2020.eu/erddap/tabledap/brizo.insert?'
  times = ",".join(df['time'].values)
  plc=",".join(df['platformcode'].values)
  mis=",".join(df['mission'].values)
  lat=",".join(df['latitude'].values)
  lon=",".join(df['longitude'].values)
  temp=",".join(df['temperature'].values)
  n=0
  url = (f'https://data-nautilos-h2020.eu/erddap/tabledap/brizo.insert?platformcode=[{plc}]&mission=[{mis}]&time=[{times}]&latitude=[{lat}]&longitude=[{lon}]&temperature=[{temp}]&author=brizouser_bR1z0u2ER')
  r = requests.get(url)
  print(f'{r.status_code}')
  print('Invio a ERDDAP riuscito')
  os.remove(csv_url)

def convert_coordinate(coord):
    coord = float(coord)
    sign = 1
    if coord < 0:
        sign = -1
    degrees = int(abs(coord) // 100)
    minutes = (abs(coord) % 100) / 60
    result = sign * (degrees + minutes)
    return result

def get_brizo_data (row):
    # Effettua la richiesta HTTP e gestisce eventuali errori
    now = datetime.utcnow()
    now_formatted = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    yesterday = now - timedelta(days=1)
    yesterday_formatted = yesterday.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    
    brizo_code = instruments_df.loc[row, 'Brizo']
    brizo_name = instruments_df.loc[row, 'Brizo']
    start_date = yesterday_formatted
    start_date = '2020-01-19T09:16:47Z'
    end_date = now_formatted
    end_date = '2024-01-19T09:16:47Z'
    api_key = instruments_df.loc[row, 'API Key']
    id_brizo = instruments_df.loc[row, 'id_brizo']
    b_name = brizo_name.lower().replace("_", "")

    CSV_URL = (f'https://api.thingspeak.com/channels/{id_brizo}/feeds.csv?start={start_date}&end={end_date}&api_key={api_key}')
    try:
        with requests.Session() as s:
            download = s.get(CSV_URL)
            download.raise_for_status()
            decoded_content = download.content.decode('utf-8')
            # Legge il file CSV in un DataFrame
            df = pd.read_csv(
                io.StringIO(decoded_content),
                delimiter=',',
                parse_dates=['created_at']
            )
            print(f'Sto processando {brizo_name}')

            # Seleziona solo le colonne di interesse e rinomina le colonne
            df = df.loc[
                (df['field1'] != 0.0) &
                (df['field2'] != 0.0) &
                (df['field8'] != 0.0),
                ['created_at', 'field1', 'field2', 'field8']
            ]
            df.columns = ['time1', 'latitude', 'longitude', 'temperature']
            
            # Salva il DataFrame in un file CSV
            df.to_csv('/home/opt/nautilosData/brizo/script_get_data/temp/1.csv', index=False)
            os.remove("/home/opt/nautilosData/brizo/script_get_data/temp/1.csv")
            # Converti la colonna 'time' in formato datetime
            df['time1'] = pd.to_datetime(df['time1'])

            # Ordina il dataframe in base alla colonna 'time'
            df = df.sort_values('time1')

            # Usa il metodo 'interpolate' per interpolare linearmente i valori mancanti nella colonna 'temperature'
            df['temperature'] = df['temperature'].interpolate()
            df['lat2'] = df['latitude'].interpolate()
            df['lon2'] = df['longitude'].interpolate()
            groups = df.groupby(['lat2', 'lon2'])

            # Per ogni gruppo, seleziona il valore della temperatura successiva più vicina
            result = groups.apply(lambda x: x['temperature'].shift(-1).iloc[np.argmin(abs(x.index - x.index[-1]))])
            result = result.reset_index()
            result.columns = ['lat2', 'lon2', 'temperature_next']
            df = df.drop(columns=['lat2', 'lon2'])
            df = df.dropna(subset=['temperature', 'latitude'])
            df['temperature'] = df['temperature'].round(4)

            # definizione della funzione lambda per generare il valore della colonna "mission"
            mission_value = lambda row: f"mission_{brizo_name}_{row['time1'].date()}"

            # applicazione della funzione alla colonna "date" e assegnazione del risultato ad una nuova colonna "mission"
            df['mission'] = df.apply(mission_value, axis=1)
            df = df.assign(platformcode=b_name)

            today = date.today()
            nome_file_csv=f'estrazione_{b_name}_{today}'

            # conversione coordinate
            for index, row in df.iterrows():
                df.at[index, 'latitude'] = convert_coordinate(row['latitude'])
                df.at[index, 'longitude'] = convert_coordinate(row['longitude'])

            df['time1'] = pd.to_datetime(df['time1'])

            df['time'] = df['time1'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

            df = df.drop(columns=['time1'])

            # Salva il dataframe risultante in un file CSV
            df.to_csv(f'{nome_file_csv}.csv', index=False)

            csv_url = (f'{nome_file_csv}.csv')
            send_to_erddap(csv_url)
    
    except requests.exceptions.RequestException as e:
        print(e)
        exit(1)

instruments_data = {
    'Brizo': ['Brizo_1', 'Brizo_2', 'Brizo_3', 'Brizo_4', 'Brizo_5'],
    'API Key': ['5CJF0937J96I3LV1',  'SU83XM2R70RPZLYS', 'WJFABTA77UAE2SKG', '3UZZUCLRBLYDELXV', '1KTSMKIFO5EC8N1E'],
    'id_brizo': [1433077, 1652965, 1652966, 1653050, 1783137]
}

instruments_df = pd.DataFrame(instruments_data)

for  row in range(len(instruments_df. index)): 
    try:
      get_brizo_data(row)
    except:
      continue
      
requests.get('https://data-nautilos-h2020.eu/erddap/setDatasetFlag.txt?datasetID=brizo&flagKey=78a061ffd275522846e914a28f9ad6683e4ee887e9a5c441cebab0f7d7354904')

