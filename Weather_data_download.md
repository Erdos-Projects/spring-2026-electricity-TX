# The code uses open-meteo api to download historical weather data for houston and, more broadly, Texas

# See all stations
python houston_weather.py --list

# See just Houston stations
python houston_weather.py --list --region Houston

# Pull all DFW stations, historical
python houston_weather.py --region DFW --mode historical --start 2024-01-01 --end 2024-12-31

# Pull single station, historical
python houston_weather.py --station KGLS --mode historical --start 2024-06-01 --end 2024-08-31

# Pull everything
python houston_weather.py --all-stations --mode historical --start 2024-06-01 --end 2024-08-31

# with output dir
python houston_weather.py --station KGLS --mode historical --start 2024-06-01 --end 2024-08-31 --output directory-path
