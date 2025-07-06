#!/usr/bin/env python
"""
Step 1 – Historical ingest & join for a single airport/month
===========================================================
*  Download **NOAA ISD‑Lite** hourly weather for the given station ‑> DataFrame `wx`
*  Download **BTS On‑Time Performance** data for the same month ‑> DataFrame `flights`
*  Bucket flight scheduled‑departure to the nearest hour and **left‑join** on
   `(flight_date, hour, origin airport)`.
*  Compute a simple `bad_wx_flag` that marks hours with either high wind speed
   or measurable precipitation.
*  Print a cross‑tab so you can eyeball that the bad‑weather share is ≈ 10–15 % in winter.

This script is deliberately **self‑contained** – no external ETL framework required –
so you can run it locally, verify the numbers, then port the logic into Airflow/dbt.
"""

from __future__ import annotations
import argparse
import gzip
import io
import json
import os
import zipfile
from pathlib import Path
import pandas as pd
import requests
from functools import lru_cache
import concurrent.futures

# Mapping of IATA airport --> (USAF, WBAN) identifiers used by ISD‑Lite files
# ISD_STATIONS: dict[str, tuple[int, int]] = {
#     "ATL": (722190, 13874),  # Atlanta Hartsfield‑Jackson , {USAF, WBAN}
#     "KJFK": (744860, 94789),
#     "KLAX": (722950, 23174),
# }

@lru_cache  # cache in-memory so repeated runs in the same process are instant
def load_isd_station_map() -> dict[str, tuple[int, int]]:
    url = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
    df = pd.read_csv(url)

    df = df[(df["CTRY"] == "US") & df["ICAO"].str.startswith("K")]
    df["IATA"] = df["ICAO"].str[1:]

    df["USAF"] = df["USAF"].astype(str)
    df["WBAN"] = df["WBAN"].astype(str)

    # keep purely-numeric IDs ≠ NOAA sentinels
    df = df[
        df["USAF"].str.isdigit() & df["WBAN"].str.isdigit()
        & (df["USAF"] != "999999") & (df["WBAN"] != "99999")
    ]

    return {r.IATA: (int(r.USAF), int(r.WBAN)) for _, r in df.iterrows()}



BTS_BASE = (
    "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{year}_{month}.zip"
)

ISD_LITE_BASE = (
    "https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/{year}/"
    "{usaf:06d}-{wban:05d}-{year}.gz"
)

ISD_COLS = [
    "year", "month", "day", "hour",
    "temp_c_tenths", "dewpt_c_tenths", "slp_hpa_tenths",
    "wind_dir_deg", "wind_speed_kt", "sky_cover_code",
    "precip_1hr_mm_tenths", "precip_6hr_mm_tenths",
]

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------



def read_isd_lite(usaf: int, wban: int, year: int, month: int, airport_iata) -> pd.DataFrame:
    """
    Download one annual ISD-Lite file, keep only the selected month,
    return a tidy hourly DataFrame.
    """
    url = ISD_LITE_BASE.format(usaf=usaf, wban=wban, year=year)
    print(f"→ Downloading {url} ...")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    # ------------- core trick: treat file as **whitespace-delimited** -------------
    gz = io.BytesIO(resp.content)
    df = pd.read_csv(
        gzip.GzipFile(fileobj=gz),
        sep=r"\s+",
        header=None,
        names=ISD_COLS,
        na_values=[-9999],
        dtype="Int64",            # nullable ints; avoids the int32 cast issue
    )
    # ------------------------------------------------------------------------------
    df = df[df["month"] == month]
    if df.empty:
        raise RuntimeError("Month filter removed all rows – check parsing.")
    # Build a proper UTC timestamp index.
    df["ts"] = pd.to_datetime(
        {
            "year":  df["year"],
            "month": df["month"],
            "day":   df["day"],
            "hour":  df["hour"],
        },
        utc=True,
    )
    df = df.set_index("ts").sort_index()
    df["flight_date"] = df.index.date
    df["hour"] = df.index.hour
    df["station"] = airport_iata[-3:]  # ← NEW
    df["ts_utc"] = df.index

    df["wx_score_raw"] = (
            (df["wind_speed_kt"] > 30)
            | (df["precip_1hr_mm_tenths"].fillna(0) > 0)
            | (df["sky_cover_code"] >= 8)
    ).fillna(False).astype("int8")

    df.rename(columns={"wx_score_raw": "wx_score"}, inplace=True)
    return df[["flight_date", "hour", "station", "wx_score", "ts_utc"]]


# ---------------------------------------------------------------------------
#  Robust BTS monthly downloader: tries Reporting first, then Marketing
# ---------------------------------------------------------------------------

CANONICAL = {
    "FL_DATE": ["FL_DATE", "FLIGHTDATE"],

    "OP_UNIQUE_CARRIER": [
        "OP_UNIQUE_CARRIER",           # Reporting files
        "REPORTING_AIRLINE",
        "IATA_CODE_REPORTING_AIRLINE",
        "MKT_UNIQUE_CARRIER",          # Marketing files (older header)
        "MARKETING_AIRLINE_NETWORK",   # ← NEW
        "IATA_CODE_MARKETING_AIRLINE", # ← NEW
        "OPERATING_AIRLINE",           # ← NEW (trailing space is trimmed)
    ],

    "TAIL_NUM"     : ["TAIL_NUM", "TAIL_NUMBER"],
    "ORIGIN"       : ["ORIGIN"],
    "DEST"         : ["DEST"],
    "DEP_TIME"     : ["DEP_TIME", "WHEELS_OFF", "DEPTIME"],
    "CRS_DEP_TIME" : ["CRS_DEP_TIME", "CRSDEPTIME"],
    "DEP_DELAY"    : ["DEP_DELAY", "DEPDELAY"],
    "ARR_DELAY"    : ["ARR_DELAY", "ARRDELAY"],
}

URL_PATTERNS = [
    # try Reporting-Carrier first
    ("Reporting",
     "https://transtats.bts.gov/PREZIP/"
     "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{y}_{m}.zip"),
    # fallback to Marketing-Carrier
    ("Marketing",
     "https://transtats.bts.gov/PREZIP/"
     "On_Time_Marketing_Carrier_On_Time_Performance_Beginning_January_2018_{y}_{m}.zip"),
]

def _download_zip(url: str) -> bytes | None:
    r = requests.get(url, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.content

def read_bts_flights(year: int, month: int) -> pd.DataFrame:
    """Return trimmed DF from whichever BTS file (Reporting or Marketing) exists."""
    zbytes = None
    for label, pattern in URL_PATTERNS:
        url = pattern.format(y=year, m=month)
        print(f"→ Trying {label} file … ", end="")
        zbytes = _download_zip(url)
        if zbytes:
            print("found.")
            break
        print("not found.")
    else:
        raise RuntimeError(f"No BTS ZIP found for {year}-{month:02d} in either dataset.")

    with zipfile.ZipFile(io.BytesIO(zbytes)) as z:
        csv_name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        with z.open(csv_name) as f:
            df = pd.read_csv(f, low_memory=False)

    df.columns = df.columns.str.strip().str.upper()

    rename_map, missing = {}, []
    for canon, aliases in CANONICAL.items():
        found = next((a for a in aliases if a in df.columns), None)
        if found:
            rename_map[found] = canon
        else:
            missing.append(canon)
    if missing:
        raise RuntimeError("BTS CSV missing expected logical fields: " + ", ".join(missing))

    return df.rename(columns=rename_map)[list(CANONICAL)]


# ---------------------------------------------------------------------------
# Main routine
# ---------------------------------------------------------------------------

def main(year: int, month: int, airport_iata: str = "ATL") -> None:
    print(f"=== Step 1 ingest & join – {airport_iata} {year}-{month:02d} ===\n")
    flights = read_bts_flights(year, month)
    print("Flight rows:", len(flights))

    station_map = load_isd_station_map()
    airports = pd.unique(pd.concat([flights["ORIGIN"], flights["DEST"]]))
    airports = [a for a in airports if a in station_map]

    def fetch_ap(ap: str) -> pd.DataFrame:
        usaf, wban = station_map[ap]
        try:
            return read_isd_lite(usaf, wban, year, month, ap)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                print(f"⚠️  No ISD-Lite file for {ap} ({usaf}-{wban}); skipping.")
                return pd.DataFrame()  # empty – will be dropped later
            raise  # re-raise unexpected errors

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            wx_tables = list(pool.map(fetch_ap, airports))

    wx_tables = [t for t in wx_tables if not t.empty]
    wx = pd.concat(wx_tables, ignore_index=True)
    print("Weather rows:", len(wx))

    flights["flight_date"] = pd.to_datetime(flights["FL_DATE"]).dt.date

    # CRS_DEP_TIME can be 5-digit (e.g.  2359) or NaN.  Force int -> hour 0-23
    flights["hour"] = (
            pd.to_numeric(flights["CRS_DEP_TIME"], errors="coerce")
            .fillna(-100)  # sentinel → becomes -1 → drops in merge
            .astype(int)
            // 100
    ).astype("int8")

    # Keep only the columns needed for the join
    wx_hourly = wx[["flight_date", "hour", "station", "wx_score"]]

    # Merge: left‑join flights → weather (if no match leave NaN → assume good weather)
    # ---------------- ORIGIN join ----------------
    flights = flights.merge(
        wx_hourly,
        left_on=["flight_date", "hour", "ORIGIN"],
        right_on=["flight_date", "hour", "station"],
        how="left",
        suffixes=("", "_ORIGWX"),
    )

    # ---------------- DEST join ------------------
    flights = flights.merge(
        wx_hourly,
        left_on=["flight_date", "hour", "DEST"],
        right_on=["flight_date", "hour", "station"],
        how="left",
        suffixes=("", "_DESTWX"),
    )

    # -------------- combined flag ---------------
    flights["bad_wx_flag"] = (
            flights["wx_score"].fillna(0).astype("int8")
            | flights["wx_score_DESTWX"].fillna(0).astype("int8")
    )

    # ------------------------------------------------------------------
    # Quick QC output
    # ------------------------------------------------------------------
    crosstab = pd.crosstab(flights["bad_wx_flag"], flights["ARR_DELAY"] > 30)
    print("\nBad-weather × >30 min arrival-delay cross-tab:\n")
    print(crosstab)

    share_bad = flights["bad_wx_flag"].mean() * 100
    print(f"\nBad-weather share: {share_bad:0.1f} % (rule-of-thumb winter 10-15 %)\n")

    out_path = Path(f"files/joined_sample_{airport_iata}_{year}_{month:02d}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    flights.to_parquet(out_path, index=False)
    print("Saved joined sample →", out_path)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Step 1 ingest + join for one airport/month")
    p.add_argument("year", type=int, help="four‑digit year, e.g. 2024")
    p.add_argument("month", type=int, help="month number 1‑12")
    p.add_argument("airport", type=str, default="ATL", nargs="?", help="IATA code (default KJFK)")
    args = p.parse_args()

    main(args.year, args.month, args.airport.upper())
