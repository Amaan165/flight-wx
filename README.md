# ✈️ flight-wx

**Weather–Flight Delay Monitor**
An end-to-end pipeline that combines FAA flight performance data with NOAA weather reports to identify bad-weather–exposed flights, aircraft, and carriers. Focused initially on JFK, the project supports both historical analysis and live ingestion readiness.

---

## Environment setup

### Create via `environment.yml`

Save the snippet below as `environment.yml` at the repo root and run the two commands that follow.

```yaml
name: flight-wx
channels:
  - conda-forge
dependencies:
  # core
  - python>=3.10
  - pandas>=2.2
  - numpy
  - pyarrow             # fast Parquet IO
  - duckdb
  - requests
  - tqdm
  # optional / dev
  - jupyterlab
  - pip
  - pip:
      - python-dotenv    # manage API keys if needed
```

```bash
conda env create -f environment.yml   # or: mamba env create -f environment.yml
conda activate flight-wx
```

---

## 🚀 Project Goals

* Join FAA **BTS On-Time Performance** data with hourly **NOAA ISD-Lite** weather logs.
* Flag flights experiencing adverse conditions at **departure or arrival** (e.g., wind > 30 kt, precipitation).
* Analyze patterns in weather-induced delay risk across **airlines**, **aircraft**, and **time-of-day**.
* Provide a clean foundation for building:

  * Real-time ingestion from OpenSky + METAR feeds.
  * A delay-prediction model.
  * An interactive operations dashboard.

---

## 📁 Data Sources

| Source                       | Type        | Link                                                                            |
| ---------------------------- | ----------- | ------------------------------------------------------------------------------- |
| 🛫 FAA BTS Flight Data       | Monthly CSV | [transtats.bts.gov](https://transtats.bts.gov/OT_Delay/OT_DelayCause1.asp?pn=1) |
| 🌦️ NOAA ISD-Lite Weather    | Hourly GZIP | [ncei.noaa.gov](https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/)              |
| 🛁 (Planned) OpenSky ADS-B   | JSON API    | [opensky-network.org](https://opensky-network.org/apidoc/)                      |
| 🌐 (Planned) Real-time METAR | JSON API    | [weather.gov](https://www.weather.gov/documentation/services-web-api)           |

---

## ⚙️ Project Structure

```
wxflight/
├── data/
│   ├── bronze/       ← raw downloaded data (zip, gz)
│   ├── silver/       ← joined Parquet files (by airport & month)
├── step1.py          ← ingest + join script (BTS + ISD-Lite)
├── utils.py          ← helpers for download, parsing, and merging
├── README.md
└── environment.yml   ← conda setup
```

---

## ✅ Step 1: Static Ingest & Join

Run this for a given month and airport (e.g. JFK, Dec 2023):

```bash
python step1.py 2023 12 KJFK
```

This will:

* Download hourly weather data for KJFK
* Download BTS on-time performance (reporting or marketing carrier)
* Join weather to both origin and destination of each flight
* Create `bad_wx_flag = 1` when adverse conditions are present
* Save a tidy joined Parquet for modeling or dashboard use

---

## 🦚 Coming Soon

* `step2_live_ingest.py`: Live ingestion of OpenSky + METAR into appendable tables
* `step3_modeling.ipynb`: Predict delay probability using carrier, time, and weather
* `dashboard/`: Superset views + SQL queries for visualizing bad-weather exposure

---

## 🛠️ Setup

```bash
conda env create -f environment.yml
conda activate flight-wx
```

Required packages:

* `pandas`, `requests`, `pyarrow`, `duckdb`
* (Optional: `plotly`, `superset`, `spark` for later stages)

---

## 📊 Example: Bad Weather × Arrival Delay

```
ARR_DELAY     False   True
bad_wx_flag
0            505093   49087
1             14356    1858
```

→ \~2.8% of flights were exposed to bad weather at JFK in Dec 2023.

---

