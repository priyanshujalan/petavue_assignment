import pandas as pd
import json
import math
from pathlib import Path

def nearest_time(time):
  if time%100 >= 30:
    if time>2300:
        return 0
    return math.ceil(time / 100) * 100
  else:
    if time==2400:
        return 0
    return math.floor(time / 100) * 100

def search_list_of_dicts(list_of_dicts, key, value):
    for item in list_of_dicts:
        if item.get(key) == value:
            return item
    return None

def get_csv_files(folder_path):
    folder = Path(folder_path)
    csv_files = list(folder.rglob("*.csv"))
    return csv_files

def get_csv_file(file_path):
    df = pd.read_csv(file_path, low_memory=False)
    return df

def get_json_file(file_path):
    df = pd.read_json(file_path)
    return df

output_filename = 'merged_csv.csv'
airports_to_consider = ['ATL', 'CLT', 'DEN', 'DFW', 'EWR', 'IAH', 'JFK', 'LAS', 'LAX', 'MCO', 'MIA', 'ORD', 'PHX', 'SEA', 'SFO']
columns_to_keep_weather = ['windspeedKmph', 'winddirDegree', 'weatherCode', 'precipMM', 'visibility', 'pressure',
                           'cloudcover', 'DewPointF', 'WindGustKmph', 'tempF', 'WindChillF', 'humidity', 'time', 'date',
                           'airport']
columns_to_keep_weather_dest = []
columns_to_keep_weather_origin = []
for keys in columns_to_keep_weather:
    columns_to_keep_weather_origin.append(f"{keys}Origin")
    columns_to_keep_weather_dest.append(f"{keys}Dest")


columns_to_keep_flights = ['FlightDate', 'Quarter', 'Year', 'Month', 'DayofMonth', 'DepTime', 'DepDel15', 'CRSDepTime',
                   'DepDelayMinutes', 'Origin', 'Dest', 'ArrTime', 'CRSArrTime', 'ArrDel15', 'ArrDelayMinutes']

empty_df = pd.DataFrame(columns=(columns_to_keep_flights + columns_to_keep_weather_origin + columns_to_keep_weather_dest))
empty_df.to_csv(output_filename, index=False)


flights_data_files = get_csv_files("data/flights")
flights_data_files = flights_data_files

for flights_data_file in flights_data_files:
    print('Inside Loop')
    df_flight = get_csv_file(flights_data_file)
    df_selected = df_flight[columns_to_keep_flights]
    for index, row in df_selected.iterrows():
        if (row['Origin'] not in airports_to_consider
                or row['Dest'] not in airports_to_consider
                or math.isnan(row['DepTime'])):
            continue
        origin_weather_filename = f"data/weather/{row['Origin']}/{row['Year']}-{row['Month']}.json"
        dest_weather_filename = f"data/weather/{row['Dest']}/{row['Year']}-{row['Month']}.json"
        with open(origin_weather_filename, 'r') as file:
            data = json.load(file)
            df_required = pd.DataFrame(data['data']['weather'])

            desired_row = df_required[df_required['date'] == row['FlightDate']]
            hour_details = search_list_of_dicts(desired_row['hourly'].iloc[0], 'time', str(nearest_time(row['DepTime'])))

            for keys in columns_to_keep_weather[0:len(columns_to_keep_weather)-2]:
                df_selected.at[index, keys+'Origin'] = hour_details[keys]

            df_selected.at[index, 'dateOrigin'] = desired_row['date'].iloc[0]
            df_selected.at[index, 'airportOrigin'] = data['data']['request'][0]['query'][0:3]

        with open(dest_weather_filename, 'r') as file:
            data = json.load(file)
            df_required = pd.DataFrame(data['data']['weather'])

            desired_row = df_required[df_required['date'] == row['FlightDate']]
            hour_details = search_list_of_dicts(desired_row['hourly'].iloc[0], 'time', str(nearest_time(row['ArrTime'])))

            for keys in columns_to_keep_weather[0:len(columns_to_keep_weather)-2]:
                df_selected.at[index, keys+'Dest'] = hour_details[keys]

            df_selected.at[index, 'dateDest'] = desired_row['date'].iloc[0]
            df_selected.at[index, 'airportDest'] = data['data']['request'][0]['query'][0:3]

    df_selected = df_selected.dropna(subset=['airportOrigin'])
    df_selected.to_csv(output_filename, mode='a', header=False, index=False)
    print(flights_data_file, 'completed.')
    del df_flight
