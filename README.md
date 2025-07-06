# ✈️ Weather-Flight Delay Monitor (`flight-wx`)

A robust data engineering pipeline that links U.S. domestic **flight performance** data with corresponding **hourly weather conditions**, and enriches each flight with **aircraft metadata** (manufacturer, model) via FAA registry. The goal is to understand which aircraft, routes, or airports operate under consistently adverse weather and how this correlates with delay metrics.

---

## ✅ Problem Motivation

Airlines frequently experience delays due to weather — but **which flights operate reliably even in bad weather?** This project builds a clean, reproducible pipeline to ingest, enrich, and join:

* ✈️ **Flight performance logs** (U.S. BTS On-Time Reporting)
* 🌤️ **Weather observations** (NOAA ISD-Lite hourly measurements)
* 🛩️ **Aircraft info** via tail number (FAA Aircraft Registry)

The final dataset can be used to:

* Visualize airport-level weather impact
* Model delay risk per aircraft or route
* Track performance under adverse meteorological conditions

---


## 🔁 Step 1: Ingest & Join

Run using either IATA code or free-text:

```bash
python step1.py 2023 12 JFK
python step1.py 2023 12 "new york"
```

### 🔽 What it does

1. **Flight Performance Data**:

   * Downloads monthly BTS zip (Reporting → fallback to Marketing)
   * Extracts key fields: FL\_DATE, DEP\_DELAY, ARR\_DELAY, ORIGIN, TAIL\_NUM, etc.

2. **Weather Data**:

   * Uses `isd-history.csv` to map airport → (USAF, WBAN)
   * Downloads NOAA ISD-Lite gz files for all airports used in that month
   * Flags "bad weather" hours using thresholds:

     * Wind speed ≥ 25 knots
     * Precipitation (mm) > 0
     * Cloud ceiling below 3000 ft

3. **FAA Aircraft Metadata**:

   * Pulls tail number → Manufacturer / Model
   * Uses FAA aircraft registry CSV export (via direct URL)
   * Maps `TAIL_NUM` to `MFR_NAME` + `MODEL_CODE`

4. **Join Everything**:

   * Merges flights × weather (on date/hour)
   * Merges aircraft metadata using tail number
   * Stores output to `joined_sample_<IATA>_<YYYY>_<MM>.parquet`

---

## 🧠 Features & Enhancements

* ✅ Dynamic IATA resolution via fuzzy match ("los angeles" → LAX)
* ✅ Auto-fallback from Reporting to Marketing BTS files
* ✅ Resilient ISD download: skips missing .gz without failure
* ✅ FAA tail registry fallback if download times out
* ✅ Clear download progress / count of stations fetched
* ✅ Select from top-k IATA matches interactively or via `--pick`
* ✅ Caches large lookups (FAA, airport-codes)
* ✅ Supports ICAO, IATA, and free-text

---

## Environment setup

```bash
conda env create -f environment.yml
conda activate flight-wx
```

Required packages:

* `pandas`, `requests`, `pyarrow`, `duckdb`
* (Optional: `plotly`, `superset`, `spark` for later stages)

---


## 📊 Example Output

After a successful run:

```bash
ARR_DELAY     False  True
bad_wx_flag
0            505093  49087
1             14356   1858
```

This shows how many flights were delayed (>30 min) in good vs. bad weather conditions.

---

## 🏗️ Planned Extensions

### 🧩 Step 2: Real-time Ingestion

* Integrate with FAA SWIM or FlightAware API for live flight data
* Track near-real-time impact of weather

### 📈 Step 3: ML Modeling

* Build classification models for delay likelihood
* Use weather, airline, route, aircraft type as features
* Output delay-risk scores per tail / route / carrier

### 📊 Step 4: Dashboard

* Visualize which aircraft models fly most in bad weather
* Heatmaps of airport-level weather impact
* Tail-level reliability charts

---

## ⚙️ Setup Instructions

### 1. Clone + Create Conda Env

```bash
git clone https://github.com/Amaan165/flight-wx.git
cd flight-wx
conda env create -f environment.yml
conda activate flight-wx
```

### 2. Run First Ingest

You can run the ingestion for any airport and month in several flexible ways:

#### Using Exact IATA Code (3-letter)

```bash
python step1.py 2023 12 JFK      # Standard IATA
python step1.py 2023 12 KJFK     # ICAO-style
```

#### Using Natural Language (fuzzy match)

```bash
python step1.py 2023 12 "new york"
```

If multiple matching airports are found (e.g. JFK, LGA, EWR), you'll be prompted to pick one interactively.

To skip the prompt and select a specific match automatically:

```bash
python step1.py 2023 12 "new york" --pick 2  
```

The script will:

* Download BTS flight data for that month
* Resolve airports dynamically from input
* Fetch ISD-Lite weather logs for **all departure airports** in the month
* Join flights + weather + tail-number metadata
* Output to: `filesjoined_sample_<IATA>_<YYYY>_<MM>.parquet`

---

### 3. Data Sources

* ✈️ [BTS On-Time Performance](https://transtats.bts.gov/OT_Delay/OT_DelayCause1.asp?pn=1) — monthly flight logs
* 🌦️ [NOAA ISD-Lite](https://www.ncei.noaa.gov/data/global-hourly/doc/isd-lite-format.txt) — hourly station weather
* 🛩️ [FAA Registry](https://registry.faa.gov/aircraftinquiry/) — N-Number → Manufacturer, Model
* 🌍 [OpenFlights Airport Metadata](https://github.com/datasets/airport-codes) — location info

---

### 4. Output Schema

The final joined dataset includes:

| Column        | Description                         |
| ------------- | ----------------------------------- |
| `FL_DATE`     | Flight date                         |
| `ORIGIN`      | Origin airport IATA                 |
| `DEP_DELAY`   | Departure delay (min)               |
| `ARR_DELAY`   | Arrival delay (min)                 |
| `DEP_TIME`    | Actual departure (local HHMM)       |
| `TAIL_NUM`    | Aircraft tail number (N-code)       |
| `mfr_name`    | Manufacturer (Boeing, Airbus, etc.) |
| `wx_score`    | Computed weather severity score     |
| `bad_wx_flag` | 1 if weather was "bad" at departure |

---


## 🛠️ Future Goals

* [ ] Add unit tests for weather scoring
* [ ] Add DuckDB dashboard preview
* [ ] Parallelize station downloads across CPUs
* [ ] Add `step2.py` for real-time ingestion
* [ ] Integrate with Airflow or Dagster pipeline