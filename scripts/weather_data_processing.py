#This script will process all the available monthly
# weather data file to create a region-wide average dataset
# for houston and texas

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from tx_stations import *
from houston_weather import *

def weather_processing():
    #Houston region weather stations
    station_code = [x for x in STATIONS.keys() if STATIONS[x]['region']== 'Houston']


    # 1. Get your paths (as Path objects for easy naming)
    directory = Path("data/raw/weather/")
    paths = [f for f in directory.rglob("*.csv") if f.parent != directory]
    print(f'working in {directory}')


    month = '' #for picking a smaller slice of the dataset base on string in the filename, default is that the code will run through every csv file
    print(f'picking all stations with "{month}"')

    texas_all_stations = pd.concat([pd.read_csv(f) for f in paths if month in f.stem], ignore_index=True)

    print('main dataframe initialised')

    #columns to drop from the dataset
    columns_to_drop = ['dew_point_f','feels_like_f',
                    'snow_in','rain_in','cloud_cover_pct',
                    'wind_dir_deg','wind_speed_mph',
                    'visibility_mi', 'wmo_code'                   
                    ]

    #setting datetime to datetime object and reordering
    texas_all_stations.datetime = pd.to_datetime(texas_all_stations.datetime)
    texas_all_stations = texas_all_stations.sort_values(by="datetime", ascending=True)

    #drop features and create a pandas index object to select numerical features
    texas_all_stations = texas_all_stations.drop(columns=columns_to_drop,errors='ignore')
    numerical_features = texas_all_stations.select_dtypes(include='number').columns

    print('datetime set, columns dropped')


    #slice of houston region stations
    houston_all_stations = texas_all_stations.loc[texas_all_stations['station_code'].isin(station_code)]

    print('houston dataframe initialised')

    # create average values for texas
    texas_avg = (
        texas_all_stations.groupby('datetime')[numerical_features]
        .mean()
        .reset_index()
    )
    texas_avg.insert(1,column='station_code',value='Texas_avg')
    texas_avg.to_csv('data/raw/weather/texas_avg.csv',index=False)


    #create standard deviation for texas
    texas_stdev = (
        texas_all_stations.groupby('datetime')[numerical_features]
        .std()
        .reset_index()
    )
    texas_stdev.insert(1,column='station_code',value='Texas_stdev')
    texas_stdev.to_csv('data/raw/weather/texas_stdev.csv',index=False)

    print(f'texas_avg, texas_stdev written to {directory}')

    # create average values for houston
    houston_avg = (
        houston_all_stations.groupby('datetime')[numerical_features]
        .mean()
        .reset_index()
    )
    houston_avg.insert(1,column='station_code',value='Houston_avg')
    houston_avg.to_csv('data/raw/weather/houston_avg.csv',index=False)


    #create standard deviation for houston
    houston_stdev = (
        houston_all_stations.groupby('datetime')[numerical_features]
        .std()
        .reset_index()
    )
    houston_stdev.insert(1,column='station_code',value='Houston_stdev')
    houston_stdev.to_csv('data/raw/weather/houston_stdev.csv',index=False)

    print(f'houston_avg, houston_stdev written to {directory}')


def pivot_weather(path: Path) -> None:
    df = pd.read_csv(path, parse_dates=['datetime'])
    
    weather_cols = df.select_dtypes(include='number').columns
    
    df['date'] = df['datetime'].dt.date
    df['hour'] = df['datetime'].dt.hour
    
    pivoted_dfs = []
    for col in weather_cols:
        pivot = df.pivot_table(index=['date', 'station_code'], columns='hour', values=col)
        pivot.columns = [f'{col}_h{str(h).zfill(2)}' for h in pivot.columns]
        pivoted_dfs.append(pivot)
    
    result = pd.concat(pivoted_dfs, axis=1).reset_index().rename(columns={'date': 'datetime'})
    
    output_path = path.parent / (path.stem + '_daily.csv')
    result.to_csv(output_path, index=False)

# Usage: pivot_weather(Path('data/weather.csv'))