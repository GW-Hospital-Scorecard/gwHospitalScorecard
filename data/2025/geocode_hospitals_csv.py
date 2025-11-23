#!/usr/bin/env python3
"""
CSV geocoder for Leaflet.js using US Census (no API key).

- Reads GWFinalSpreadsheet.csv (or any similar CSV).
- Uses Street_Address, City, State, ZIP_Code to build addresses.
- Calls US Census Geocoder.
- Writes Latitude, Longitude, Geo_Source, Geo_Confidence.
- Has fallbacks:
    1) Street + City + State + ZIP
    2) Hospital_Name + City + State + ZIP
    3) City + State + ZIP (city centroid)
- Writes any still-unmatched rows to unmatched_geocode.csv
"""

import csv
import time
import argparse
import requests
from typing import Tuple, Optional, Dict, List

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
CENSUS_BENCHMARK = "Public_AR_Current"

# ---- THESE MUST MATCH THE HEADERS IN YOUR CSV ----
STREET_COL = "Street_Address"
CITY_COL   = "City"
STATE_COL  = "State"
ZIP_COL    = "ZIP_Code"
HOSP_COL   = "Hospital_Name"
# --------------------------------------------------


def join_nonempty(parts: List[str], sep: str = ", ") -> str:
    return sep.join(p for p in parts if p)


def build_primary_address(row: Dict[str, str]) -> str:
    """Street + City + State + ZIP."""
    street = str(row.get(STREET_COL, "")).strip()
    city   = str(row.get(CITY_COL, "")).strip()
    state  = str(row.get(STATE_COL, "")).strip()
    zipc   = str(row.get(ZIP_COL, "")).strip()
    return join_nonempty([street, city, state, zipc])


def build_hospital_address(row: Dict[str, str]) -> str:
    """Hospital_Name + City + State + ZIP."""
    name  = str(row.get(HOSP_COL, "")).strip()
    city  = str(row.get(CITY_COL, "")).strip()
    state = str(row.get(STATE_COL, "")).strip()
    zipc  = str(row.get(ZIP_COL, "")).strip()
    return join_nonempty([name, city, state, zipc])


def build_city_address(row: Dict[str, str]) -> str:
    """City + State + ZIP (city centroid)."""
    city  = str(row.get(CITY_COL, "")).strip()
    state = str(row.get(STATE_COL, "")).strip()
    zipc  = str(row.get(ZIP_COL, "")).strip()
    return join_nonempty([city, state, zipc])


def census_geocode(address: str, timeout: int = 20) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """Return (lat, lon, confidence-ish) using US Census (no key)."""
    if not address:
        return None, None, None
    params = {"address": address, "benchmark": CENSUS_BENCHMARK, "format": "json"}
    r = requests.get(CENSUS_URL, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json() or {}
    matches = (data.get("result") or {}).get("addressMatches") or []
    if not matches:
        return None, None, None
    top = matches[0]
    coords = top.get("coordinates") or {}
    lon = coords.get("x")
    lat = coords.get("y")
    conf = (top.get("tigerLine") or {}).get("side")
    return lat, lon, conf


def main():
    parser = argparse.ArgumentParser(description="CSV geocoder for hospitals (Census).")
    parser.add_argument("--infile", required=True, help="Input CSV path")
    parser.add_argument("--outfile", required=True, help="Output CSV path")
    parser.add_argument("--sleep", type=float, default=0.25, help="Delay between requests (seconds)")
    parser.add_argument("--round", dest="round_decimals", type=int, default=5,
                        help="Round coordinates to N decimals (default: 5)")
    parser.add_argument("--start", type=int, default=0, help="Start row index (0-based)")
    parser.add_argument("--limit", type=int, default=None, help="Max rows to process")
    args = parser.parse_args()

    # 1) Read CSV using Windows/Excel-friendly encoding
    print(f"Reading: {args.infile}")
    with open(args.infile, newline="", encoding="cp1252", errors="replace") as fin:
        reader = csv.DictReader(fin)
        rows = list(reader)

    if not rows:
        print("No data rows found in CSV.")
        return

    headers = list(rows[0].keys())
    print("Detected headers:")
    print(headers)

    # Check that our configured columns exist
    for col in (STREET_COL, CITY_COL, STATE_COL, ZIP_COL, HOSP_COL):
        if col not in headers:
            print(f"ERROR: Column '{col}' not found in CSV headers.")
            print("If your header is spelled differently, edit STREET_COL/CITY_COL/STATE_COL/ZIP_COL/HOSP_COL at the top.")
            return

    # 2) Prepare output fieldnames
    fieldnames = headers[:]
    for col in ("Latitude", "Longitude", "Geo_Source", "Geo_Confidence"):
        if col not in fieldnames:
            fieldnames.append(col)

    # 3) Process and geocode
    end = len(rows) if args.limit is None else min(len(rows), args.start + args.limit)
    cache: Dict[str, Tuple[Optional[float], Optional[float], Optional[str]]] = {}
    unmatched_rows: List[Dict[str, str]] = []

    print(f"Total rows: {len(rows)}  |  Processing rows {args.start} to {end - 1}")

    with open(args.outfile, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for idx in range(args.start, end):
            row = rows[idx]

            primary_addr = build_primary_address(row)

            if idx < 5:
                print(f"Row {idx} primary address: {primary_addr}")

            lat = row.get("Latitude")
            lon = row.get("Longitude")
            conf = row.get("Geo_Confidence")
            source = row.get("Geo_Source") or "census"

            if not (lat and lon) and primary_addr:
                # Use cache if seen before
                cache_key = primary_addr
                if cache_key in cache:
                    lat, lon, conf = cache[cache_key]
                else:
                    # ---- Attempt 1: street address ----
                    lat, lon, conf = census_geocode(primary_addr)
                    if lat is not None and lon is not None:
                        source = "census_primary"
                        print(f"[OK 1] row#{idx} {primary_addr} -> {lat}, {lon}")
                    else:
                        print(f"[NO MATCH 1] row#{idx} {primary_addr}")

                        # ---- Attempt 2: hospital name ----
                        hosp_addr = build_hospital_address(row)
                        if hosp_addr and hosp_addr != primary_addr:
                            lat, lon, conf = census_geocode(hosp_addr)
                            if lat is not None and lon is not None:
                                source = "census_hospital"
                                print(f"[OK 2] row#{idx} {hosp_addr} -> {lat}, {lon}")
                            else:
                                print(f"[NO MATCH 2] row#{idx} {hosp_addr}")

                        # ---- Attempt 3: city centroid ----
                        if lat is None or lon is None:
                            city_addr = build_city_address(row)
                            if city_addr:
                                lat, lon, conf = census_geocode(city_addr)
                                if lat is not None and lon is not None:
                                    source = "census_city"
                                    print(f"[OK 3] row#{idx} {city_addr} -> {lat}, {lon}")
                                else:
                                    print(f"[NO MATCH 3] row#{idx} {city_addr}")

                    cache[cache_key] = (lat, lon, conf)
                    time.sleep(args.sleep)

            # Round coordinates if present
            try:
                if lat is not None:
                    lat = round(float(lat), args.round_decimals)
                if lon is not None:
                    lon = round(float(lon), args.round_decimals)
            except Exception:
                pass

            row["Latitude"] = lat
            row["Longitude"] = lon
            row["Geo_Source"] = source
            row["Geo_Confidence"] = conf

            if lat is None or lon is None:
                # Save info for manual fix
                row_copy = dict(row)
                row_copy["Unmatched_Primary_Address"] = primary_addr
                row_copy["Unmatched_Hospital_Address"] = build_hospital_address(row)
                row_copy["Unmatched_City_Address"] = build_city_address(row)
                unmatched_rows.append(row_copy)

            writer.writerow(row)

            if idx > args.start and (idx - args.start) % 200 == 0:
                print(f"...processed {idx - args.start} rows")

    print(f"Done. Wrote: {args.outfile}")

    # Write unmatched rows to separate CSV for manual geocoding
    if unmatched_rows:
        unmatched_path = "unmatched_geocode.csv"
        print(f"Writing {len(unmatched_rows)} unmatched rows to {unmatched_path}")
        u_fieldnames = list(unmatched_rows[0].keys())
        with open(unmatched_path, "w", newline="", encoding="utf-8") as uf:
            uw = csv.DictWriter(uf, fieldnames=u_fieldnames)
            uw.writeheader()
            for r in unmatched_rows:
                uw.writerow(r)


if __name__ == "__main__":
    main()
