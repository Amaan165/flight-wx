# Flight   Delay  &  Weather  Monitor  — Step   1  MVP

A quick-start repo scaffold for ingesting hourly NOAA weather and BTS on‑time performance data, joining them for a single airport/month, and computing a first‑pass **`bad_wx_flag`**.

---

## 1    Prerequisites

* **Python  3.10** or newer.
* **Conda / mamba** distribution (recommended) — Miniconda, Anaconda, or Micromamba.
* 1–2   GB free disk for raw downloads + Parquet outputs.

---

## 2    Environment setup

### 2.1    Create via `environment.yml`

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

> **Tip  💡**    If you already have an environment, you can *merge* instead:
>
> ```bash
> conda env update -f environment.yml --prune
> ```

### 2.2    Manual install (fallback)

If you prefer installing packages ad‑hoc:

```bash
conda create -n flight-wx python=3.10 pandas numpy requests pyarrow duckdb tqdm jupyterlab
conda activate flight-wx
pip install python-dotenv
```

---

## 3    Running Step  1

With the environment active and `step1_ingest_katl.py` in the repo root:

```bash
python step1_ingest_katl.py --year 2024 --month 1 --airport KATL
```

\###  3.1    What the script does

1. Downloads the BTS on‑time CSV for **January  2024** (≈  250  MB).
2. Pulls NOAA **ISD‑Lite** hourly weather for Atlanta (station 722190‑13874).
3. Buckets scheduled‑departure to the nearest hour and joins weather on
   `(flight_date, sched_dep_hour, ORIGIN)`.
4. Flags `bad_wx_flag` when visibility  <  5   km, wind  >  30   kt, low ceiling, or precip.
5. Persists two Parquet files under `data/`:

   * `flights_2024_01_KATL.parquet`
   * `flights_wx_join_2024_01_KATL.parquet`

A summary line prints the share of flights in bad weather so you can sanity‑check the month (typical winter value ≈  12  %).

---

## 4    Project structure (after Step   1)

```
.
├── data/
│     ├── raw/
│     │     ├── isd_lite_202401_KATL.gz
│     │     └── bts_otp_202401.csv
│     ├── flights_2024_01_KATL.parquet
│     └── flights_wx_join_2024_01_KATL.parquet
├── step1_ingest_katl.py
├── environment.yml
└── README.md
```

---

## 5    Next milestones

| Step  | Goal                                                                        |
| ----- | --------------------------------------------------------------------------- |
| **2** | Parameterise the ingest script to any airport + month, push to Airflow DAG. |
| **3** | Add live ADS‑B & METAR streaming via Kafka → DuckDB.                        |
| **4** | Build aggregation models (`agg_tail_day`, `fleet_exposure`).                |
| **5** | Stand‑up Apache Superset dashboard.                                         |

Happy hacking  🚀 — open an issue or ping me if you hit snags!
