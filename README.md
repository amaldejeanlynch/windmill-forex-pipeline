# Forex Data Pipeline

Production ETL pipeline that continuously extracts OHLCV candle data from the OANDA API and stores it in
PostgreSQL — serving as the raw data layer for downstream consumers (FastAPI services, dashboards,
feature pipelines, ML models).

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-336791?logo=postgresql&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-3.0_Cloud-024DFD?logo=prefect&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-2.0-150458?logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-red)
![License](https://img.shields.io/badge/license-MIT-green)

---

## What It Does

Runs on a Railway Cron schedule. Each run queries the audit log per instrument/granularity pair,
fetches only new candles since the last successful run, validates them, and upserts to Supabase
PostgreSQL. First run backfills from `2026-01-01 UTC`. Re-running after failure never creates duplicates.

---

## Architecture

```
OANDA REST API
      │
      ▼
┌─────────────────┐
│  OandaClient    │  Extract — date-range pagination, retries, complete candles only
└────────┬────────┘
         │ raw DataFrame
         ▼
┌─────────────────┐
│ DataValidator   │  Validate — schema, nulls, duplicates, OHLC logic
└────────┬────────┘
         │ clean DataFrame
         ▼
┌─────────────────┐
│ DatabaseManager │  Load — upsert to PostgreSQL, write audit log
└────────┬────────┘
         │
         ▼
    PostgreSQL (Supabase)
  ┌─────────────────────┐   ┌──────────────────────┐
  │  instrument_candles │   │  instrument_etl_log  │
  └─────────────────────┘   └──────────────────────┘

Orchestrated by Prefect Cloud — task-level tracking, retries, audit trail
Deployed via Railway Cron — scheduled runs, exits cleanly after each run
```

---

## Database

Hosted on Supabase (PostgreSQL 17, `eu-west-2`). Two tables managed by this pipeline.

### `instrument_candles`

Primary key: `(time, instrument, granularity)`

| Column | Type | Notes |
|--------|------|-------|
| time | timestamptz | Candle open time (UTC) |
| instrument | varchar(20) | OANDA instrument code e.g. `EUR_USD` |
| granularity | varchar(5) | M5 / M15 / M30 / H1 |
| open / high / low / close | float8 | OHLC bid prices |
| volume | int8 | Tick count for the candle |
| created_at | timestamptz | Row insertion timestamp |

Indexes: `(instrument, granularity, time)` for time-series queries; `(time)` for cross-instrument scans.

### `instrument_etl_log`

Primary key: `(run_id, instrument, granularity)`

One row per run per instrument/granularity pair. `to_date` is the resumption cursor for the next run.

| Column | Type | Notes |
|--------|------|-------|
| run_id | varchar | UUID for the pipeline run |
| instrument / granularity | varchar | Identifies the pair |
| from_date / to_date | timestamptz | Fetch window for this run |
| rows_extracted / rows_valid / rows_loaded | int | Quality audit trail |
| status | varchar | `success` or `failed` |
| error_message | text | Populated on failure, null on success |
| started_at / completed_at | timestamptz | Run timing |

### Live stats (2026-04-17)

**683,242 rows** across 22 instruments × 4 granularities = 88 active combinations. Range: `2026-01-01 → present`.

---

## Instruments & Granularities

**Granularities:** `M5` `M15` `M30` `H1`

| Category | Instruments |
|----------|-------------|
| Major FX | EUR_USD, GBP_USD, USD_JPY, USD_CHF, USD_CAD, AUD_USD, NZD_USD |
| Metals | XAU_USD, XAG_USD, XCU_USD |
| Energy | BCO_USD, WTICO_USD, NATGAS_USD |
| Equity Indices | SPX500_USD, NAS100_USD, US30_USD |
| Government Bonds | USB02Y_USD, USB05Y_USD, USB10Y_USD, USB30Y_USD, DE10YB_EUR, UK10YB_GBP |

---

## Tech Stack

| Technology | Role |
|------------|------|
| Python 3.11 | Core language — type hints, OOP |
| pandas | In-memory DataFrame for extract / validate / transform |
| SQLAlchemy 2.0 | ORM, connection pool, session management |
| psycopg2 | PostgreSQL adapter |
| Prefect 3 Cloud | Pipeline orchestration, task-level tracking, retries |
| pydantic-settings | Settings validation — fails fast on missing env vars |
| tenacity | Exponential backoff on transient API failures |
| python-json-logger | Structured JSON logs for cloud log aggregation |
| colorlog | Coloured console output for local runs |

---

## Project Structure

```
forex-pipeline/
├── run.py                 # Entry point — setup_logging() then run_pipeline()
├── railway.toml           # Railway deployment config (Cron Job, NIXPACKS)
├── src/
│   ├── config.py          # Centralised settings — pydantic-settings, env var validation
│   ├── logging_config.py  # JSON file + coloured console handlers, timed_operation ctx manager
│   ├── oanda_client.py    # OandaClient — paginated fetch, retry/backoff, complete-only filter
│   ├── validator.py       # DataValidator — 4 quality checks, immutable ValidationResult
│   ├── database.py        # DatabaseManager — upsert, bulk cursor query, audit log write
│   ├── models.py          # SQLAlchemy ORM: instrument_candles + instrument_etl_log
│   ├── utils.py           # format_oanda_timestamp, df_to_candle_records
│   └── pipeline.py        # Prefect flow: extract_task → validate_task → load_task per pair
├── data/raw/              # Raw data snapshots (git-ignored)
├── logs/                  # Runtime log files (git-ignored)
└── requirements.txt       # Pinned dependencies
```

---

## Environment Variables

```
# OANDA API
OANDA_API_TOKEN=your_token
OANDA_ACCOUNT_ID=your_account_id
OANDA_BASE_URL=https://api-fxpractice.oanda.com

# Database — Supabase direct connection (port 5432, not pooler port 6543)
DATABASE_URL=postgresql+psycopg2://postgres:[PASSWORD]@db.[ref].supabase.co:5432/postgres?sslmode=require

# Prefect Cloud — optional, pipeline runs locally without these
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/{account_id}/workspaces/{workspace_id}
PREFECT_API_KEY=your_prefect_api_key
```

Tables are created automatically on first run via `Base.metadata.create_all()`.

---

## Running

```bash
python run.py
```

`setup_logging()` initialises before the Prefect flow so JSON file logs and coloured console output
both work alongside Prefect Cloud's task-level tracking. With Prefect credentials set, every run is
visible at [app.prefect.cloud](https://app.prefect.cloud).

---

## Data Quality Checks

Every DataFrame passes 4 validation rules before touching the database:

| Check | Rule | Action on Failure |
|-------|------|-------------------|
| Schema | All required columns present, correct dtypes | Pipeline aborts — no data written |
| Nulls | No missing OHLCV values | Rows dropped; error raised if > 5% |
| Duplicates | No repeated timestamps per instrument | Rows deduplicated; error raised if > 10% |
| OHLC Logic | `high >= low`, all prices > 0 | Invalid rows dropped |

---

## Design Decisions

**1. `complete=True` filter at API response time**

OANDA marks the currently-forming candle as `complete=False`. Filtering at source — before constructing
the DataFrame — prevents ingesting a partial candle stored with provisional OHLCV values that would be
silently overwritten on the next run with different data. Without this, the last candle of every fetch
window would be unreliable in the database.

**2. Date-range pagination with `GRANULARITY_SECONDS` cursor**

Count-based fetching returns the same N candles regardless of elapsed time. Date-range pagination fetches
exactly the candles between `from` and `to`. The `GRANULARITY_SECONDS` map advances the cursor by one
candle step between pages — preventing both gaps (missed candles) and overlap (duplicate work). This is
the only way to guarantee contiguous, non-redundant coverage when the number of new candles is unknown.

**3. Single bulk query replaces 88 per-pair queries**

`get_all_last_fetch_dates` uses PostgreSQL's `DISTINCT ON (instrument, granularity) ... ORDER BY to_date DESC`
to return every resumption cursor in one round-trip. Without this, naively determining where to resume would
require one query per pair — 88 queries per run with the current config. The pipeline does O(1) dict lookups
against the result.

**4. Composite natural PK on `instrument_candles`: `(time, instrument, granularity)`**

A surrogate `id` would require a separate `UNIQUE` constraint to make upserts safe — two indexes for the same
uniqueness guarantee. The natural composite PK means `ON CONFLICT (time, instrument, granularity) DO NOTHING`
maps directly to the PK with no extra index. Every pipeline run is fully idempotent.

**5. Per-pair independent failure isolation**

Each instrument/granularity combination runs as its own Prefect task sequence. A failure in one pair — API
error, validation rejection — is caught, logged, and written to `instrument_etl_log` with `status=failed`
without raising to the flow level. The remaining combinations continue unaffected. A transient timeout on
one bond instrument never blocks the FX pairs.

---

## About

Built by [Amal Dejean Lynch](https://www.linkedin.com/in/amal-dejean-lynch/) — Data Engineer & Python Developer, London.

**GitHub:** [github.com/amaldejeanlynch](https://github.com/amaldejeanlynch)
