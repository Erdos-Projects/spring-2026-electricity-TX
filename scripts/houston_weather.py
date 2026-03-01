"""
Hourly Weather Data Puller — Texas ASOS Stations
Uses Open-Meteo API (free, no API key required)

Station registry is in tx_stations.py — edit that file to add/remove stations.

Install dependencies:
    pip install requests pandas

Usage examples:
    # Forecast for default station (KIAH), one CSV per month
    python houston_weather.py

    # Forecast for a specific station
    python houston_weather.py --station KHOU

    # Forecast for ALL Texas stations
    python houston_weather.py --all-stations

    # Forecast for a region
    python houston_weather.py --region "Rio Grande Valley"

    # Historical for one station
    python houston_weather.py --station KAUS --mode historical --start 2024-01-01 --end 2024-12-31

    # Historical for all stations in a region
    python houston_weather.py --region DFW --mode historical --start 2024-06-01 --end 2024-08-31

    # List available stations
    python houston_weather.py --list
    python houston_weather.py --list --region Houston

    # Single combined file instead of monthly splits
    python houston_weather.py --station KGLS --single-file
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta

from tx_stations import STATIONS, DEFAULT_STATION, list_stations, print_registry

TIMEZONE = "America/Chicago"

HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "cloud_cover",
    "visibility",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "surface_pressure",
    "weather_code",
]

COLUMN_RENAMES = {
    "temperature_2m":       "temp_f",
    "apparent_temperature": "feels_like_f",
    "dew_point_2m":         "dew_point_f",
    "relative_humidity_2m": "humidity_pct",
    "precipitation":        "precip_in",
    "rain":                 "rain_in",
    "snowfall":             "snow_in",
    "cloud_cover":          "cloud_cover_pct",
    "visibility":           "visibility_m",
    "wind_speed_10m":       "wind_speed_mph",
    "wind_direction_10m":   "wind_dir_deg",
    "wind_gusts_10m":       "wind_gust_mph",
    "surface_pressure":     "pressure_hpa",
    "weather_code":         "wmo_code",
}


# ── Shared post-processing ────────────────────────────────────────────────────
def _process(df: pd.DataFrame, station_code: str) -> pd.DataFrame:
    """Rename columns, coerce types, prepend station identifier columns."""
    df.rename(columns={"time": "datetime"}, inplace=True)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.rename(columns=COLUMN_RENAMES, inplace=True)

    for col in df.columns:
        if col != "datetime":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["visibility_mi"] = (df["visibility_m"] / 1609.34).round(2)
    df.drop(columns=["visibility_m"], inplace=True)

    info = STATIONS[station_code]
    df.insert(1, "station_code", station_code)
    df.insert(2, "station_name", info["name"])
    df.insert(3, "city",         info["city"])
    df.insert(4, "region",       info["region"])

    return df


# ── Fetch functions ───────────────────────────────────────────────────────────
def fetch_forecast(station_code: str = DEFAULT_STATION, days_ahead: int = 3) -> pd.DataFrame:
    """Fetch hourly forecast (up to 16 days) from Open-Meteo."""
    info = STATIONS[station_code]
    print(f"  [{station_code}] Fetching {days_ahead}-day forecast — {info['name']} ({info['city']})...")

    params = {
        "latitude":           info["lat"],
        "longitude":          info["lon"],
        "hourly":             HOURLY_FIELDS,
        "wind_speed_unit":    "mph",
        "temperature_unit":   "fahrenheit",
        "precipitation_unit": "inch",
        "timezone":           TIMEZONE,
        "forecast_days":      days_ahead,
    }
    r = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=15)
    r.raise_for_status()
    df = pd.DataFrame(r.json()["hourly"])
    return _process(df, station_code)


def fetch_historical(station_code: str = DEFAULT_STATION,
                     start_date: str = None,
                     end_date: str = None) -> pd.DataFrame:
    """Fetch hourly historical data from Open-Meteo Archive API."""
    info = STATIONS[station_code]
    print(f"  [{station_code}] Fetching historical — {info['name']} ({start_date} -> {end_date})...")

    params = {
        "latitude":           info["lat"],
        "longitude":          info["lon"],
        "start_date":         start_date,
        "end_date":           end_date,
        "hourly":             HOURLY_FIELDS,
        "wind_speed_unit":    "mph",
        "temperature_unit":   "fahrenheit",
        "precipitation_unit": "inch",
        "timezone":           TIMEZONE,
    }
    r = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json()["hourly"])
    return _process(df, station_code)


# ── WMO code lookup ───────────────────────────────────────────────────────────
def wmo_code_description(code) -> str:
    """Map WMO weather interpretation codes to human-readable descriptions. Null-safe."""
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return "Unknown"
    wmo_map = {
        0:  "Clear sky",          1:  "Mainly clear",       2:  "Partly cloudy",
        3:  "Overcast",           45: "Fog",                 48: "Icy fog",
        51: "Light drizzle",      53: "Moderate drizzle",    55: "Dense drizzle",
        61: "Slight rain",        63: "Moderate rain",       65: "Heavy rain",
        71: "Slight snow",        73: "Moderate snow",       75: "Heavy snow",
        80: "Slight showers",     81: "Moderate showers",    82: "Heavy showers",
        95: "Thunderstorm",       96: "Thunderstorm w/ slight hail",
        99: "Thunderstorm w/ heavy hail",
    }
    return wmo_map.get(int(code), f"Code {int(code)}")


# ── Output helpers ────────────────────────────────────────────────────────────
def _add_condition(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["condition"] = df["wmo_code"].apply(wmo_code_description)
    return df


def save_monthly_csvs(df: pd.DataFrame, output_dir: str = "weather_output",
                      station_code: str = None) -> list[str]:
    """
    Split df by year-month, write one CSV per month.
    File naming: {STATION_CODE}_{YYYY-MM}.csv  e.g. KIAH_2024-03.csv
    """
    os.makedirs(output_dir, exist_ok=True)
    df = _add_condition(df)
    prefix = station_code or df["station_code"].iloc[0]

    df = df.copy()
    df["_ym"] = df["datetime"].dt.to_period("M")
    written = []
    for period, group in df.groupby("_ym"):
        group = group.drop(columns=["_ym"])
        filename = os.path.join(output_dir, f"{prefix}_{period}.csv")
        group.to_csv(filename, index=False)
        print(f"  Saved: {filename}  ({len(group)} rows)")
        written.append(filename)
    return written


def save_single_csv(df: pd.DataFrame, output_dir: str = "weather_output",
                    station_code: str = None) -> str:
    """Write df to a single combined CSV."""
    os.makedirs(output_dir, exist_ok=True)
    df = _add_condition(df)
    prefix = station_code or df["station_code"].iloc[0]
    filename = os.path.join(output_dir, f"{prefix}_weather.csv")
    df.to_csv(filename, index=False)
    print(f"  Saved: {filename}  ({len(df)} rows)")
    return filename


# ── Console summary ───────────────────────────────────────────────────────────
def print_summary(df: pd.DataFrame, n_rows: int = 24) -> None:
    code   = df["station_code"].iloc[0]
    name   = df["station_name"].iloc[0]
    region = df["region"].iloc[0]
    print(f"\n{'='*90}")
    print(f"  {code} -- {name}  [{region}]  |  {df['datetime'].iloc[0].strftime('%Y-%m-%d')}  "
          f"({len(df)} hours total)")
    print(f"{'='*90}")
    print(f"{'Time':<20} {'Temp F':>7} {'FeelsLike':>10} {'Humidity':>9} "
          f"{'Wind mph':>9} {'Gusts':>7} {'Precip':>7}  Condition")
    print("-" * 90)
    for _, row in df.head(n_rows).iterrows():
        print(
            f"{str(row['datetime']):<20} "
            f"{row['temp_f']:>7.1f} "
            f"{row['feels_like_f']:>10.1f} "
            f"{row['humidity_pct']:>8.0f}% "
            f"{row['wind_speed_mph']:>9.1f} "
            f"{row['wind_gust_mph']:>7.1f} "
            f"{row['precip_in']:>7.3f}  "
            f"{wmo_code_description(row['wmo_code'])}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch hourly weather for Texas ASOS stations via Open-Meteo.\n"
                    "Station registry is defined in tx_stations.py.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--station", default=DEFAULT_STATION,
        help=f"ICAO station code (default: {DEFAULT_STATION}). "
             "Ignored if --all-stations or --region is set."
    )
    parser.add_argument(
        "--all-stations", action="store_true",
        help="Fetch data for every station in tx_stations.py"
    )
    parser.add_argument(
        "--region", type=str, default=None,
        help="Fetch all stations in a named region "
             "(e.g. 'Houston', 'DFW', 'Panhandle', 'Rio Grande Valley'). "
             "Run --list to see all regions."
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Print the station registry and exit. Combine with --region to filter."
    )
    parser.add_argument(
        "--mode", choices=["forecast", "historical"], default="forecast",
        help="'forecast' (default) or 'historical'"
    )
    parser.add_argument(
        "--days", type=int, default=3,
        help="Days ahead for forecast mode (default: 3, max: 16)"
    )
    parser.add_argument(
        "--start", type=str, default=None,
        help="Start date for historical mode (YYYY-MM-DD, default: 30 days ago)"
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="End date for historical mode (YYYY-MM-DD, default: yesterday)"
    )
    parser.add_argument(
        "--output-dir", type=str, default="weather_output",
        help="Directory to write CSV files (default: weather_output/)"
    )
    parser.add_argument(
        "--single-file", action="store_true",
        help="Write one combined CSV per station instead of splitting by month"
    )
    parser.add_argument(
        "--no-save", action="store_true",
        help="Print summary to console only, skip saving CSVs"
    )
    args = parser.parse_args()

    # --list: print registry and exit
    if args.list:
        print_registry(region=args.region)
        print(f"\nTotal: {len(list_stations(args.region))} station(s)")
        raise SystemExit(0)

    # Resolve target station list
    if args.all_stations:
        target_codes = list(STATIONS.keys())
    elif args.region:
        target_codes = [s["code"] for s in list_stations(region=args.region)]
        if not target_codes:
            print(f"No stations found for region '{args.region}'. Run --list to see all regions.")
            raise SystemExit(1)
    else:
        code = args.station.upper()
        if code not in STATIONS:
            print(f"Unknown station '{code}'. Run --list to see available stations.")
            raise SystemExit(1)
        target_codes = [code]

    # Date defaults for historical mode
    start = args.start or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end   = args.end   or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\nMode       : {args.mode.upper()}")
    print(f"Stations   : {len(target_codes)}  "
          f"({', '.join(target_codes[:8])}{'...' if len(target_codes) > 8 else ''})")
    if args.mode == "historical":
        print(f"Date range : {start} -> {end}")
    print(f"Output dir : {args.output_dir}/\n")

    all_files = []
    for code in target_codes:
        try:
            if args.mode == "forecast":
                df = fetch_forecast(station_code=code, days_ahead=args.days)
            else:
                df = fetch_historical(station_code=code, start_date=start, end_date=end)

            print_summary(df)

            if not args.no_save:
                print()
                if args.single_file:
                    f = save_single_csv(df, output_dir=args.output_dir, station_code=code)
                    all_files.append(f)
                else:
                    files = save_monthly_csvs(df, output_dir=args.output_dir, station_code=code)
                    all_files.extend(files)

        except Exception as e:
            print(f"  [ERROR] {code}: {e}")

        print()

    if not args.no_save and all_files:
        print(f"Done. {len(all_files)} CSV file(s) written to: {args.output_dir}/")
