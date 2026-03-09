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


# ── Batch crawler ─────────────────────────────────────────────────────────────
BATCH_SIZE     = 2   # stations per batch
BATCH_INTERVAL = 120  # seconds to wait between batches


def region_subdir(output_dir: str, region: str) -> str:
    """Return (and create) a subdirectory named after the region."""
    safe = region.replace(" ", "_").replace("/", "-")
    path = os.path.join(output_dir, safe)
    os.makedirs(path, exist_ok=True)
    return path


def crawl_all_stations(
    start_date:  str,
    end_date:    str,
    output_dir:  str = "weather_output",
    batch_size:  int = BATCH_SIZE,
    interval:    int = BATCH_INTERVAL,
    single_file: bool = False,
    resume:      bool = True,
) -> None:
    """
    Fetch historical data for every station in tx_stations.py, 
    `batch_size` stations at a time with `interval` seconds between batches.
    CSVs are stored under output_dir/{Region}/.

    Args:
        start_date:  'YYYY-MM-DD'
        end_date:    'YYYY-MM-DD'
        output_dir:  root output directory
        batch_size:  stations per batch (default 4)
        interval:    seconds to pause between batches (default 30)
        single_file: one combined CSV per station vs. one per month
        resume:      skip stations that already have output files
    """
    import time

    all_codes  = list(STATIONS.keys())
    total      = len(all_codes)
    done_file  = os.path.join(output_dir, ".completed_stations.txt")

    # Load already-completed stations if resuming
    completed: set[str] = set()
    if resume and os.path.exists(done_file):
        with open(done_file) as f:
            completed = {line.strip() for line in f if line.strip()}

    remaining = [c for c in all_codes if c not in completed]

    if not remaining:
        print("All stations already completed. Delete weather_output/.completed_stations.txt to re-run.")
        return

    # Split into batches
    batches = [remaining[i:i + batch_size] for i in range(0, len(remaining), batch_size)]

    print(f"\n{'='*65}")
    print(f"  Texas Historical Crawl")
    print(f"  Date range  : {start_date} -> {end_date}")
    print(f"  Stations    : {len(remaining)} remaining / {total} total")
    print(f"  Batches     : {len(batches)}  ({batch_size} stations each, {interval}s apart)")
    print(f"  Output dir  : {output_dir}/{{Region}}/")
    print(f"  Resume      : {'yes — skipping already-completed stations' if resume else 'no'}")
    print(f"  Press Ctrl-C to stop. Progress is saved; re-run to resume.")
    print(f"{'='*65}\n")

    total_files  = 0
    batch_num    = 0

    for batch in batches:
        batch_num   += 1
        batch_start  = time.time()
        done_in_batch = 0

        print(f"[Batch {batch_num}/{len(batches)}]  "
              f"{datetime.now().strftime('%H:%M:%S')}  —  {', '.join(batch)}")
        print(f"{'-'*65}")

        for code in batch:
            info = STATIONS[code]
            out  = region_subdir(output_dir, info["region"])
            try:
                df = fetch_historical(station_code=code,
                                      start_date=start_date,
                                      end_date=end_date)
                if single_file:
                    files = [save_single_csv(df, output_dir=out, station_code=code)]
                else:
                    files = save_monthly_csvs(df, output_dir=out, station_code=code)

                total_files += len(files)
                done_in_batch += 1

                # Mark as completed
                with open(done_file, "a") as f:
                    f.write(code + "\n")
                completed.add(code)

            except Exception as e:
                print(f"  [ERROR] {code}: {e}")

        elapsed = time.time() - batch_start
        remaining_batches = len(batches) - batch_num
        eta_sec = remaining_batches * (elapsed + interval)
        eta_str = str(timedelta(seconds=int(eta_sec)))

        print(f"\n  Batch {batch_num} done in {elapsed:.1f}s "
              f"({done_in_batch}/{len(batch)} succeeded).")
        print(f"  Progress: {len(completed)}/{total} stations complete. "
              f"ETA to finish: ~{eta_str}")

        if batch_num < len(batches):
            print(f"  Waiting {interval}s before next batch...\n")
            time.sleep(interval)

    print(f"\n{'='*65}")
    print(f"  Crawl complete.")
    print(f"  {len(completed)} stations fetched, {total_files} CSV files written.")
    print(f"  Output: {output_dir}/{{Region}}/")
    print(f"{'='*65}")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Fetch historical hourly weather for all Texas ASOS stations.\n"
            "Processes 4 stations per batch with a 30-second pause between batches\n"
            "to stay within Open-Meteo rate limits. Output is organized by region.\n\n"
            "Station registry is defined in tx_stations.py."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--start", type=str,
        default=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
        help="Historical start date YYYY-MM-DD (default: 30 days ago)"
    )
    parser.add_argument(
        "--end", type=str,
        default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Historical end date YYYY-MM-DD (default: yesterday)"
    )
    parser.add_argument(
        "--output-dir", type=str, default="weather_output",
        help="Root output directory. Subdirs are created per region. (default: weather_output/)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=BATCH_SIZE,
        help=f"Stations per batch (default: {BATCH_SIZE})"
    )
    parser.add_argument(
        "--interval", type=int, default=BATCH_INTERVAL,
        help=f"Seconds between batches (default: {BATCH_INTERVAL})"
    )
    parser.add_argument(
        "--single-file", action="store_true",
        help="Write one combined CSV per station instead of one per month"
    )
    parser.add_argument(
        "--no-resume", action="store_true",
        help="Re-fetch all stations even if already completed (ignores progress file)"
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Print the station registry and exit"
    )
    parser.add_argument(
        "--region", type=str, default=None,
        help="Filter --list output by region name"
    )

    args = parser.parse_args()

    if args.list:
        print_registry(region=args.region)
        print(f"\nTotal: {len(list_stations(args.region))} station(s)")
        raise SystemExit(0)

    try:
        crawl_all_stations(
            start_date=args.start,
            end_date=args.end,
            output_dir=args.output_dir,
            batch_size=args.batch_size,
            interval=args.interval,
            single_file=args.single_file,
            resume=not args.no_resume,
        )
    except KeyboardInterrupt:
        print("\n\nCrawl interrupted. Re-run to resume from where you left off.")
