# Texas ASOS Weather Puller — Usage Guide

Fetches hourly weather data for Texas ASOS stations from the [Open-Meteo](https://open-meteo.com) archive API. Free to use, no API key required.

## Files

| File | Purpose |
|---|---|
| `houston_weather.py` | Main script — crawling, fetching, saving |
| `tx_stations.py` | Station registry — add/remove/edit stations here |

## Setup

```bash
pip install requests pandas
```

Both files must be in the same directory.

---

## Modes

### 1. Full Texas Crawl *(default)*

Fetches historical data for **every station** in `tx_stations.py`, processing 4 stations per batch with a 30-second pause between batches to stay within Open-Meteo rate limits.

```bash
python houston_weather.py --start 2024-01-01 --end 2024-12-31
```

Progress is saved automatically. If the script is interrupted, re-run the same command and it resumes from where it left off. To start over from scratch, use `--no-resume` or delete `weather_output/.completed_stations.txt`.

**Options:**

| Flag | Default | Description |
|---|---|---|
| `--start` | 30 days ago | Historical start date `YYYY-MM-DD` |
| `--end` | Yesterday | Historical end date `YYYY-MM-DD` |
| `--output-dir` | `weather_output` | Root directory for CSV output |
| `--batch-size` | `4` | Stations per batch |
| `--interval` | `30` | Seconds to wait between batches |
| `--single-file` | off | Write one combined CSV per station instead of one per month |
| `--no-resume` | off | Re-fetch all stations, ignoring prior progress |

---

### 2. One-shot: Single Station

Fetches one station and exits. Supports both historical and forecast modes.

```bash
# Historical (default)
python houston_weather.py --station KIAH --start 2024-01-01 --end 2024-12-31

# Forecast (next N days)
python houston_weather.py --station KHOU --mode forecast --days 7
```

**Options:**

| Flag | Default | Description |
|---|---|---|
| `--station` | — | ICAO station code, e.g. `KIAH` |
| `--mode` | `historical` | `historical` or `forecast` |
| `--start` | 30 days ago | Start date for historical mode |
| `--end` | Yesterday | End date for historical mode |
| `--days` | `3` | Days ahead for forecast mode (max 16) |
| `--single-file` | off | One combined CSV instead of one per month |
| `--output-dir` | `weather_output` | Root output directory |

---

### 3. One-shot: Entire Region

Fetches all stations within a named region and exits.

```bash
python houston_weather.py --region Houston --start 2024-06-01 --end 2024-08-31
python houston_weather.py --region "Rio Grande Valley" --start 2024-01-01 --end 2024-12-31
python houston_weather.py --region DFW --mode forecast --days 5
```

Region names are case-sensitive. See the [Regions](#regions) section below for all valid names.

---

### 4. List Stations

Print the station registry and exit.

```bash
# All stations
python houston_weather.py --list

# Filter by region
python houston_weather.py --list --region Panhandle
```

---

## Output Structure

CSVs are organized into subdirectories by region under the output directory:

```
weather_output/
  Houston/
    KIAH_2024-01.csv
    KIAH_2024-02.csv
    KHOU_2024-01.csv
    KGLS_2024-01.csv
  DFW/
    KDFW_2024-01.csv
    KDAL_2024-01.csv
  Panhandle/
    KAMA_2024-01.csv
  Far_West_Texas/
    KELP_2024-01.csv
  ...
  .completed_stations.txt   ← crawl progress tracker (hidden file)
```

With `--single-file`, monthly splits are replaced by one file per station:

```
weather_output/
  Houston/
    KIAH_weather.csv
    KHOU_weather.csv
```

### CSV Columns

Every CSV includes these columns:

| Column | Description |
|---|---|
| `datetime` | Observation timestamp (America/Chicago) |
| `station_code` | ICAO code, e.g. `KIAH` |
| `station_name` | Full station name |
| `city` | Nearest city |
| `region` | Texas region grouping |
| `temp_f` | Air temperature (°F) |
| `feels_like_f` | Apparent temperature (°F) |
| `dew_point_f` | Dew point (°F) |
| `humidity_pct` | Relative humidity (%) |
| `precip_in` | Total precipitation (inches) |
| `rain_in` | Rainfall (inches) |
| `snow_in` | Snowfall (inches) |
| `cloud_cover_pct` | Cloud cover (%) |
| `wind_speed_mph` | Wind speed (mph) |
| `wind_dir_deg` | Wind direction (degrees) |
| `wind_gust_mph` | Wind gusts (mph) |
| `pressure_hpa` | Surface pressure (hPa) |
| `visibility_mi` | Visibility (miles) |
| `wmo_code` | WMO weather interpretation code |
| `condition` | Human-readable weather condition |

---

## Regions

| Region | Example Stations |
|---|---|
| `Houston` | KIAH, KHOU, KGLS, KSGR, KEFD, KCXO, KLBX |
| `DFW` | KDFW, KDAL, KFTW, KAFW, KADS, KRBD |
| `Austin` | KAUS, KACT, KTPL, KILE |
| `San Antonio` | KSAT, KSKF, KRND, KHDO |
| `East Texas` | KBPT, KTYR, KGGG, KTXK |
| `Central Texas` | KABI, KBWD, KCLL, KSEP, KJCT |
| `North Texas` | KSPS, KUTH, KCDS |
| `South Texas` | KCRP, KALI, KNQI, KLRD |
| `Rio Grande Valley` | KBRO, KHRL, KMFE |
| `West Texas` | KMAF, KLBB, KMDD, KBPG, KSJT |
| `Far West Texas` | KELP, KMRF, KPRS, KPEX, KINK |
| `Panhandle` | KAMA, KPPA, KPVW, KHRX |

---

## Station Registry (`tx_stations.py`)

To add a station, append an entry to the `STATIONS` dict:

```python
"KABC": {
    "name": "My Airport",
    "city": "City Name",
    "lat":  30.1234,
    "lon": -97.5678,
    "elev": 150,        # meters above sea level
    "region": "Austin", # must match an existing region name, or create a new one
},
```

The default station (used when no `--station` flag is passed) is set at the top of `tx_stations.py`:

```python
DEFAULT_STATION = "KIAH"
```

---

## Rate Limiting Notes

Open-Meteo's free tier allows roughly **10,000 requests/day** per IP. Each station fetch is one request. At the default settings (4 stations per batch, 30s between batches):

- ~70 stations = ~18 batches = **~9 minutes** for a full Texas crawl
- Daily re-runs of a full crawl use ~70 requests/day — well within limits
- For large date ranges (multi-year), each station is still only one request

If you hit rate limits, increase `--interval` or decrease `--batch-size`.

---

## Examples

```bash
# Full Texas crawl, last year
python houston_weather.py --start 2024-01-01 --end 2024-12-31

# Full Texas crawl, conservative rate limiting
python houston_weather.py --start 2023-01-01 --end 2024-12-31 --batch-size 2 --interval 60

# Resume an interrupted crawl (default behavior)
python houston_weather.py --start 2024-01-01 --end 2024-12-31

# Force re-fetch everything from scratch
python houston_weather.py --start 2024-01-01 --end 2024-12-31 --no-resume

# Just the Houston region, monthly CSVs
python houston_weather.py --region Houston --start 2024-01-01 --end 2024-12-31

# Just the Houston region, one file per station
python houston_weather.py --region Houston --start 2024-01-01 --end 2024-12-31 --single-file

# Single station, historical
python houston_weather.py --station KELP --start 2022-01-01 --end 2024-12-31

# Single station, 10-day forecast
python houston_weather.py --station KAMA --mode forecast --days 10

# Save to a custom directory
python houston_weather.py --start 2024-01-01 --end 2024-12-31 --output-dir /data/texas_weather

# See all stations
python houston_weather.py --list

# See only Far West Texas stations
python houston_weather.py --list --region "Far West Texas"
```
