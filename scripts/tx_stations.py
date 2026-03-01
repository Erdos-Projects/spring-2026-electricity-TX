"""
Texas ASOS Station Registry
Source: FAA / NOAA ASOS network, coordinates from official station metadata.
All stations are K-prefixed ICAO identifiers with known lat/lon.

Usage:
    from tx_stations import STATIONS, DEFAULT_STATION, get_station, list_stations

Each entry:
    code  → ICAO station identifier  (e.g. "KIAH")
    name  → Human-readable name      (e.g. "Houston Intercontinental")
    city  → Nearest city
    lat   → Latitude  (decimal degrees N)
    lon   → Longitude (decimal degrees W, negative)
    elev  → Elevation (meters above sea level)
    region → Informal Texas region grouping
"""

DEFAULT_STATION = "KIAH"

STATIONS: dict[str, dict] = {

    # ── Houston / Southeast ───────────────────────────────────────────────────
    "KIAH": {"name": "Houston Intercontinental",    "city": "Houston",          "lat": 29.9902, "lon": -95.3368, "elev":  33, "region": "Houston"},
    "KHOU": {"name": "Houston Hobby",               "city": "Houston",          "lat": 29.6454, "lon": -95.2789, "elev":  14, "region": "Houston"},
    "KSGR": {"name": "Sugar Land Regional",         "city": "Sugar Land",       "lat": 29.6223, "lon": -95.6565, "elev":  23, "region": "Houston"},
    "KEFD": {"name": "Ellington Field",             "city": "Houston",          "lat": 29.6073, "lon": -95.1588, "elev":  14, "region": "Houston"},
    "KCXO": {"name": "Conroe North Houston",        "city": "Conroe",           "lat": 30.3518, "lon": -95.4143, "elev":  73, "region": "Houston"},
    "KLBX": {"name": "Angleton / Lake Jackson",     "city": "Lake Jackson",     "lat": 29.1086, "lon": -95.4627, "elev":  13, "region": "Houston"},
    "KDWH": {"name": "David Wayne Hooks Memorial",  "city": "Houston",          "lat": 30.0618, "lon": -95.5528, "elev":  46, "region": "Houston"},
    "KGLS": {"name": "Galveston",                   "city": "Galveston",        "lat": 29.2653, "lon": -94.8604, "elev":   6, "region": "Houston"},
    "KBYY": {"name": "Bay City Municipal",          "city": "Bay City",         "lat": 28.9733, "lon": -95.8635, "elev":  33, "region": "Houston"},
    "KVCT": {"name": "Victoria Regional",           "city": "Victoria",         "lat": 28.8526, "lon": -96.9185, "elev":  35, "region": "Houston"},

    # ── Dallas / Fort Worth / North Texas ─────────────────────────────────────
    "KDFW": {"name": "Dallas / Fort Worth Intl",    "city": "Dallas",           "lat": 32.8998, "lon": -97.0403, "elev": 182, "region": "DFW"},
    "KDAL": {"name": "Dallas Love Field",           "city": "Dallas",           "lat": 32.8471, "lon": -96.8518, "elev": 148, "region": "DFW"},
    "KADS": {"name": "Addison Airport",             "city": "Addison",          "lat": 32.9686, "lon": -96.8364, "elev": 193, "region": "DFW"},
    "KFTW": {"name": "Fort Worth Meacham Intl",     "city": "Fort Worth",       "lat": 32.8198, "lon": -97.3623, "elev": 215, "region": "DFW"},
    "KAFW": {"name": "Fort Worth Alliance",         "city": "Fort Worth",       "lat": 32.9876, "lon": -97.3188, "elev": 219, "region": "DFW"},
    "KGKY": {"name": "Arlington Municipal",         "city": "Arlington",        "lat": 32.6638, "lon": -97.0943, "elev": 192, "region": "DFW"},
    "KGPM": {"name": "Grand Prairie Municipal",     "city": "Grand Prairie",    "lat": 32.6988, "lon": -97.0467, "elev": 176, "region": "DFW"},
    "KRBD": {"name": "Dallas Executive",            "city": "Dallas",           "lat": 32.6809, "lon": -96.8682, "elev": 215, "region": "DFW"},
    "KGVT": {"name": "Greenville / Hunt County",   "city": "Greenville",       "lat": 33.0678, "lon": -96.0653, "elev": 157, "region": "DFW"},
    "KSPS": {"name": "Wichita Falls Municipal",     "city": "Wichita Falls",    "lat": 33.9888, "lon": -98.4918, "elev": 314, "region": "North Texas"},
    "KSJT": {"name": "San Angelo Regional",         "city": "San Angelo",       "lat": 31.3577, "lon": -100.4963,"elev": 578, "region": "West Texas"},
    "KABI": {"name": "Abilene Regional",            "city": "Abilene",          "lat": 32.4113, "lon": -99.6819, "elev": 546, "region": "West Texas"},
    "KBWD": {"name": "Brownwood Regional",          "city": "Brownwood",        "lat": 31.7934, "lon": -98.9564, "elev": 405, "region": "Central Texas"},
    "KGGG": {"name": "East Texas Regional",        "city": "Longview",         "lat": 32.3840, "lon": -94.7115, "elev": 107, "region": "East Texas"},
    "KTYR": {"name": "Tyler Pounds Regional",       "city": "Tyler",            "lat": 32.3541, "lon": -95.4024, "elev": 163, "region": "East Texas"},
    "KGTP": {"name": "Gatesville Municipal",        "city": "Gatesville",       "lat": 31.4227, "lon": -97.7966, "elev": 296, "region": "Central Texas"},

    # ── Austin / Central Texas ─────────────────────────────────────────────────
    "KAUS": {"name": "Austin-Bergstrom Intl",       "city": "Austin",           "lat": 30.1945, "lon": -97.6699, "elev": 160, "region": "Austin"},
    "KRYW": {"name": "Lago Vista / Rusty Allen",    "city": "Lago Vista",       "lat": 30.4986, "lon": -97.9693, "elev": 373, "region": "Austin"},
    "KATT": {"name": "Atwater / Atkinson Muni",     "city": "Atoka",            "lat": 31.1771, "lon": -97.9302, "elev": 270, "region": "Central Texas"},
    "KTPL": {"name": "Temple Draughon-Miller",      "city": "Temple",           "lat": 31.1523, "lon": -97.4078, "elev": 213, "region": "Central Texas"},
    "KACT": {"name": "Waco Regional",               "city": "Waco",             "lat": 31.6113, "lon": -97.2305, "elev": 155, "region": "Central Texas"},
    "KHLR": {"name": "Fort Cavazos (Hood AAF)",     "city": "Killeen",          "lat": 31.1387, "lon": -97.7145, "elev": 306, "region": "Central Texas"},
    "KILE": {"name": "Killeen-Fort Hood Regional",  "city": "Killeen",          "lat": 31.0858, "lon": -97.6869, "elev": 274, "region": "Central Texas"},
    "KGRK": {"name": "Gray AAF",                    "city": "Killeen",          "lat": 31.0672, "lon": -97.8289, "elev": 263, "region": "Central Texas"},
    "KBMQ": {"name": "Burnet Municipal",            "city": "Burnet",           "lat": 30.7389, "lon": -98.2387, "elev": 395, "region": "Central Texas"},
    "KLZZ": {"name": "Lampasas Municipal",          "city": "Lampasas",         "lat": 31.1062, "lon": -98.1955, "elev": 406, "region": "Central Texas"},

    # ── San Antonio / South Central ────────────────────────────────────────────
    "KSAT": {"name": "San Antonio Intl",            "city": "San Antonio",      "lat": 29.5337, "lon": -98.4698, "elev": 241, "region": "San Antonio"},
    "KSKF": {"name": "Kelly Field Annex (Lackland)","city": "San Antonio",      "lat": 29.3842, "lon": -98.5811, "elev": 203, "region": "San Antonio"},
    "KSSF": {"name": "Stinson Municipal",           "city": "San Antonio",      "lat": 29.3369, "lon": -98.4711, "elev": 193, "region": "San Antonio"},
    "KRND": {"name": "Randolph AFB",                "city": "San Antonio",      "lat": 29.5297, "lon": -98.2789, "elev": 229, "region": "San Antonio"},
    "KHDO": {"name": "Hondo Municipal",             "city": "Hondo",            "lat": 29.3595, "lon": -99.1767, "elev": 295, "region": "San Antonio"},
    "KBKD": {"name": "Stephens County",             "city": "Breckenridge",     "lat": 32.7190, "lon": -98.8902, "elev": 399, "region": "West Texas"},
    "KNQI": {"name": "NAS Kingsville",              "city": "Kingsville",       "lat": 27.5072, "lon": -97.8097, "elev":  18, "region": "South Texas"},
    "KALI": {"name": "Alice International",         "city": "Alice",            "lat": 27.7409, "lon": -98.0269, "elev":  53, "region": "South Texas"},
    "KCRP": {"name": "Corpus Christi Intl",         "city": "Corpus Christi",   "lat": 27.7704, "lon": -97.5012, "elev":  13, "region": "South Texas"},
    "KNGP": {"name": "NAS Corpus Christi",          "city": "Corpus Christi",   "lat": 27.6926, "lon": -97.2910, "elev":  18, "region": "South Texas"},
    "KPSX": {"name": "Weiser Air Park",             "city": "Polk",             "lat": 28.7278, "lon": -96.1596, "elev":  15, "region": "South Texas"},

    # ── Rio Grande Valley / South Texas ───────────────────────────────────────
    "KBRO": {"name": "Brownsville / South Padre",   "city": "Brownsville",      "lat": 25.9068, "lon": -97.4259, "elev":   7, "region": "Rio Grande Valley"},
    "KHRL": {"name": "Valley Intl (Harlingen)",     "city": "Harlingen",        "lat": 26.2285, "lon": -97.6541, "elev":  11, "region": "Rio Grande Valley"},
    "KMFE": {"name": "McAllen Miller Intl",         "city": "McAllen",          "lat": 26.1758, "lon": -98.2386, "elev":  34, "region": "Rio Grande Valley"},
    "KNOG": {"name": "Orange Grove NALF",           "city": "Orange Grove",     "lat": 27.9001, "lon": -97.9958, "elev":  71, "region": "South Texas"},
    "KLRD": {"name": "Laredo Intl",                 "city": "Laredo",           "lat": 27.5438, "lon": -99.4612, "elev": 155, "region": "South Texas"},
    "KDRT": {"name": "Del Rio Intl",                "city": "Del Rio",          "lat": 29.3742, "lon": -100.9272,"elev": 311, "region": "West Texas"},
    "KUTH": {"name": "Quanah Municipal",            "city": "Quanah",           "lat": 34.3737, "lon": -99.7195, "elev": 439, "region": "North Texas"},

    # ── El Paso / Far West Texas ───────────────────────────────────────────────
    "KELP": {"name": "El Paso Intl",                "city": "El Paso",          "lat": 31.8072, "lon": -106.3779,"elev":1194, "region": "Far West Texas"},
    "KBIF": {"name": "Biggs AAF",                   "city": "El Paso",          "lat": 31.8495, "lon": -106.3799,"elev":1200, "region": "Far West Texas"},
    "KMRF": {"name": "Marfa Municipal",             "city": "Marfa",            "lat": 30.3711, "lon": -104.0172,"elev":1481, "region": "Far West Texas"},
    "KPRS": {"name": "Presidio Lely Intl",          "city": "Presidio",         "lat": 29.6345, "lon": -104.3593,"elev": 893, "region": "Far West Texas"},
    "KPEX": {"name": "Pecos Municipal",             "city": "Pecos",            "lat": 31.3824, "lon": -103.5113,"elev": 778, "region": "Far West Texas"},
    "KMDD": {"name": "Midland Airpark",             "city": "Midland",          "lat": 31.9427, "lon": -102.1008,"elev": 861, "region": "West Texas"},
    "KMAF": {"name": "Midland Intl Air & Space",    "city": "Midland",          "lat": 31.9425, "lon": -102.2019,"elev": 875, "region": "West Texas"},
    "KODO": {"name": "Odessa / Schlemeyer Field",   "city": "Odessa",           "lat": 31.9207, "lon": -102.3877,"elev": 902, "region": "West Texas"},
    "KBPG": {"name": "Big Spring McMahon-Wrinkle",  "city": "Big Spring",       "lat": 32.2125, "lon": -101.5224,"elev": 798, "region": "West Texas"},

    # ── Panhandle / Northwest Texas ────────────────────────────────────────────
    "KAMA": {"name": "Rick Husband Amarillo Intl",  "city": "Amarillo",         "lat": 35.2194, "lon": -101.7059,"elev":1099, "region": "Panhandle"},
    "KPPA": {"name": "Pampa Perry Lefors Field",    "city": "Pampa",            "lat": 35.6130, "lon": -100.9958,"elev": 971, "region": "Panhandle"},
    "KPVW": {"name": "Plainview/Hale County",       "city": "Plainview",        "lat": 34.1849, "lon": -101.7224,"elev":1036, "region": "Panhandle"},
    "KCDS": {"name": "Childress Municipal",         "city": "Childress",        "lat": 34.4337, "lon": -100.2883,"elev": 592, "region": "North Texas"},
    "KLUBBOCK": {"name": "Lubbock Intl",            "city": "Lubbock",          "lat": 33.6636, "lon": -101.8228,"elev": 993, "region": "West Texas"},  # note: key below
    "KLBB": {"name": "Lubbock Intl",                "city": "Lubbock",          "lat": 33.6636, "lon": -101.8228,"elev": 993, "region": "West Texas"},
    "KCNM": {"name": "Carlsbad Cavern City (NM)",   "city": "Carlsbad",         "lat": 32.3372, "lon": -104.2633,"elev": 971, "region": "Far West Texas"},

    # ── East Texas ─────────────────────────────────────────────────────────────
    "KBPT": {"name": "Jack Brooks Regional",        "city": "Beaumont",         "lat": 29.9508, "lon": -94.0207, "elev":  16, "region": "East Texas"},
    "KORG": {"name": "Orange County",               "city": "Orange",           "lat": 30.0691, "lon": -93.8007, "elev":   7, "region": "East Texas"},
    "KSNY": {"name": "Sidney Municipal",            "city": "Sidney",           "lat": 32.6927, "lon": -100.9861,"elev": 534, "region": "West Texas"},
    "KTXK": {"name": "Texarkana Regional",          "city": "Texarkana",        "lat": 33.4539, "lon": -93.9909, "elev":  116, "region": "East Texas"},
    "KSEP": {"name": "Stephenville Clark Rgnl",     "city": "Stephenville",     "lat": 32.2153, "lon": -98.1777, "elev": 394, "region": "Central Texas"},
    "KJCT": {"name": "Junction Airport",            "city": "Junction",         "lat": 30.5113, "lon": -99.7737, "elev": 596, "region": "Central Texas"},
    "KINK": {"name": "Winkler County",              "city": "Wink",             "lat": 31.7796, "lon": -103.2003,"elev": 869, "region": "Far West Texas"},
    "KNKL": {"name": "Noonkester Field",            "city": "Texas City",       "lat": 29.3983, "lon": -94.9072, "elev":   2, "region": "Houston"},
    "KCLL": {"name": "Easterwood Field (College Station)", "city": "College Station", "lat": 30.5886, "lon": -96.3638, "elev":  97, "region": "Central Texas"},
    "KHRX": {"name": "Hereford Municipal",          "city": "Hereford",         "lat": 34.3992, "lon": -102.4011,"elev":1034, "region": "Panhandle"},
    "KGDJ": {"name": "Granbury Regional",           "city": "Granbury",         "lat": 32.4449, "lon": -97.8169, "elev": 253, "region": "DFW"},
    "KSEP": {"name": "Stephenville Clark Rgnl",     "city": "Stephenville",     "lat": 32.2153, "lon": -98.1777, "elev": 394, "region": "Central Texas"},
    "KLHB": {"name": "Hearne Municipal",            "city": "Hearne",           "lat": 30.8594, "lon": -96.6247, "elev":  99, "region": "Central Texas"},
    "KPOY": {"name": "Post / Garza County",         "city": "Post",             "lat": 33.1929, "lon": -101.3424,"elev": 912, "region": "West Texas"},
    "KSAF2": {"name": "Stafford Municipal",         "city": "Stafford",         "lat": 29.6119, "lon": -95.5597, "elev":  15, "region": "Houston"},
}

# Remove accidental duplicate
STATIONS.pop("KLUBBOCK", None)
STATIONS.pop("KSEP", None)   # remove second duplicate entry, keep first
# Re-add clean deduplication
_seen = {}
_clean = {}
for k, v in STATIONS.items():
    if k not in _seen:
        _clean[k] = v
        _seen[k] = True
STATIONS = _clean


def get_station(code: str) -> dict:
    """Return station metadata dict for a given ICAO code. Raises KeyError if not found."""
    code = code.upper()
    if code not in STATIONS:
        raise KeyError(f"Station '{code}' not found. Use list_stations() to see all options.")
    return {**STATIONS[code], "code": code}


def list_stations(region: str = None) -> list[dict]:
    """
    Return list of all stations (or filtered by region).
    Each dict includes the 'code' key.

    Regions: Houston, DFW, Austin, San Antonio, East Texas, Central Texas,
             South Texas, Rio Grande Valley, West Texas, Far West Texas,
             North Texas, Panhandle
    """
    results = [{"code": k, **v} for k, v in STATIONS.items()]
    if region:
        results = [s for s in results if s["region"].lower() == region.lower()]
    return sorted(results, key=lambda s: (s["region"], s["code"]))


def print_registry(region: str = None) -> None:
    """Pretty-print the station registry, optionally filtered by region."""
    stations = list_stations(region)
    current_region = None
    for s in stations:
        if s["region"] != current_region:
            current_region = s["region"]
            print(f"\n── {current_region} {'─'*(50-len(current_region))}")
            print(f"  {'Code':<8} {'Name':<40} {'City':<20} {'Lat':>8} {'Lon':>10} {'Elev(m)':>8}")
            print(f"  {'-'*7} {'-'*39} {'-'*19} {'-'*8} {'-'*10} {'-'*8}")
        print(f"  {s['code']:<8} {s['name']:<40} {s['city']:<20} "
              f"{s['lat']:>8.4f} {s['lon']:>10.4f} {s['elev']:>8}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Texas ASOS Station Registry")
    parser.add_argument("--region", type=str, default=None,
                        help="Filter by region (e.g. 'Houston', 'DFW', 'Panhandle')")
    parser.add_argument("--count", action="store_true",
                        help="Print total station count only")
    args = parser.parse_args()

    if args.count:
        print(f"Total Texas ASOS stations: {len(STATIONS)}")
    else:
        print_registry(args.region)
        print(f"\nTotal: {len(list_stations(args.region))} station(s)")
