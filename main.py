import pandas as pd
from pathlib import Path

def get_csv_files(folder_path):
    folder = Path(folder_path)
    csv_files = list(folder.rglob("*.csv"))
    return csv_files

def get_json_files(folder_path):
    folder = Path(folder_path)
    json_files = list(folder.rglob("*.json"))
    dfs = []
    for json_file in json_files:
        df = pd.read_csv(json_file)
        print(json_file, 'Added.')
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)
    return csv_files

def get_csv_file(file_path):
    df = pd.read_csv(file_path, low_memory=False)
    return df

def get_json_file(file_path):
    df = pd.read_json(file_path)
    return df

flights_data_files = get_csv_files("data/flights")
flights_data_files = flights_data_files

output_filename = 'merged_csv.csv'
columns_to_keep = ['FlightDate', 'Quarter', 'Year', 'Month', 'DayofMonth', 'DepTime', 'DepDel15', 'CRSDepTime',
                   'DepDelayMinutes', 'Origin', 'Dest', 'ArrTime', 'CRSArrTime', 'ArrDel15', 'ArrDelayMinutes']

empty_df = pd.DataFrame(columns=columns_to_keep)
empty_df.to_csv(output_filename, index=False)

for flights_data_file in flights_data_files:
    df = get_csv_file(flights_data_file)
    df_selected = df[columns_to_keep]

    df_selected.to_csv(output_filename, mode='a', header=False, index=False)
    print(flights_data_file, 'completed.')
    del df
