# Architecture

## High-level overview

```
Browser (localhost:5173)
    │  React + Vite
    │  Recharts, Tailwind CSS
    │
    │  HTTP /api/* (proxied by Vite dev server)
    ▼
FastAPI (localhost:8000)
    │  api/main.py
    │  api/routers/{prices, generation, analytics}.py
    │
    │  pd.read_parquet()
    ▼
Parquet files (data/curated/)
    │
    │  Written by ETL scripts
    ▼
AEMO data sources
    ├─ MMSDM Monthly Archive  (historical, >6 weeks old)
    └─ NEMWeb Archive/Current (recent, up to ~5 min lag)
```

---

## Backend — FastAPI (`api/`)

### `api/main.py`
App entrypoint. Registers three routers and adds CORS middleware (allows `localhost:5173`).

### `api/routers/prices.py`
Loads all `data/curated/prices/prices_*.parquet` files into a single DataFrame on each request.
Key endpoints:
- `GET /` — time-series of RRP, filterable by `start_date`, `end_date`, `region`
- `GET /summary` — latest RRP, day average, negative interval count
- `GET /meta` — date range available for a region
- `POST /backfill` — trigger MMSDM download for `{year, month}`
- `POST /sync` — auto-detect gaps and fill using `etl/auto_sync.py`

### `api/routers/generation.py`
Same pattern for SCADA data. Also loads `data/static/duid_fuel.csv` and joins it to SCADA records so the API returns fuel-labelled generation.

Battery G/L variant resolution: DUIDs like `BATT1G1` (generator mode) and `BATT1L1` (load mode) are resolved back to their base DUID `BATT1` for fuel lookup.

### `api/routers/analytics.py`
Derived analytics computed server-side from price and SCADA data:
- `GET /renewable-penetration` — resamples SCADA to 30-min intervals, computes renewable MW / total MW
- `GET /price-duration` — sorts all RRP values descending and maps to percentile, sampled to ~2000 points for chart performance

---

## Frontend — React + Vite (`frontend/`)

### Component structure
```
src/
  App.jsx                    # BrowserRouter, routes
  pages/
    Home.jsx                 # Overview KPIs + nav cards
    Prices.jsx               # RRP chart + intervals table
    GenerationMix.jsx        # Stacked fuel area chart + top units
    Analytics.jsx            # Renewable penetration + price duration
  components/
    Layout.jsx               # Sidebar + Outlet wrapper
    Sidebar.jsx              # Navigation
    MetricCard.jsx           # KPI stat card
    LoadingSpinner.jsx       # Loading state
    EmptyState.jsx           # Empty state message
    SyncButton.jsx           # Sync to Today button with status
    BackfillPanel.jsx        # Manual month/year backfill (kept, not in main UI)
  api/
    client.js                # Axios wrapper for all API calls
```

### Data flow in a page
1. `useEffect` on mount → call `getMeta()` to get available date range
2. Set default date range to the full month of latest available data
3. `useCallback` + `useEffect` on date change → fetch chart data
4. Render chart from state, show `LoadingSpinner` while fetching, `EmptyState` if empty
5. `SyncButton` on click → `POST /sync` → reload meta → refetch chart

### Vite proxy
All `/api` requests from the frontend are proxied to `http://localhost:8000` by Vite's dev server (`vite.config.js`). This means CORS is not a concern during development — requests appear same-origin to the browser.

---

## ETL pipeline (`etl/`)

### Two data source tiers

**Tier 1 — MMSDM Monthly Archive**
- URL pattern: `nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month:02}/MMSDM_Historical_Data_SQLLoader/DATA/`
- Published: ~6 weeks after month end
- Format: ZIP containing one large CSV in SQL Loader format (C/I/D row prefixes)
- Coverage: 2009 to ~6 weeks ago
- Modules: `etl/mmsdm_price.py`, `etl/mmsdm_scada.py`

**Tier 2 — NEMWeb Archive/Current**
- URL: `nemweb.com.au/Reports/Archive/DispatchIS_Reports/` and `/Dispatch_SCADA/`
- Published: ~5 minutes after each interval
- Format: Individual ZIP per 5-min interval, same SQL Loader format
- Coverage: Rolling ~3 months (Archive), last few hours (Current)
- Modules: `etl/nemweb_price.py`, `etl/nemweb_scada.py`
- Uses `ThreadPoolExecutor` with 20 workers to parallelise individual file downloads

**Smart sync (`etl/auto_sync.py`)**
Chooses the right tier automatically:
- If month ended >42 days ago → use MMSDM (one big download)
- Otherwise → use NEMWeb Archive (parallel small downloads)
- Caps at 12 months per call, returns `more_available: true` if more gaps exist

### SQL Loader format
AEMO's standard export format. Every line starts with a record type:
```
C,NEMSPDWH,,...        # header / metadata
I,DISPATCH,PRICE,5,... # column headers for PRICE table (5-min)
D,DISPATCH,PRICE,5,... # data rows
```
Parsers skip C rows, use I rows for column names (starting at field index 4), and D rows for data.

### DUID fuel mapping (`etl/build_duid_mapping.py`)
1. Downloads AEMO's Registration & Exemption List Excel workbook
2. Finds the "PU and Scheduled Loads" sheet
3. Maps each DUID to a fuel type via `classify_fuel(primary_fuel, technology_type)`
4. Applies `MANUAL_OVERRIDES` dict for units that the automated classification gets wrong (VIC1 coal and gas units)
5. Writes `data/static/duid_fuel.csv` with UTF-8 encoding

Unicode note: AEMO uses en-dashes (–) in column names. The `_norm()` function normalises all dash variants to ASCII hyphen before column matching.

---

## Data model

### Price Parquet schema
| Column | Type | Description |
|--------|------|-------------|
| `ts_start_nem` | datetime[tz=+10:00] | Start of 5-min interval |
| `ts_end_nem` | datetime[tz=+10:00] | End of 5-min interval (AEMO's SETTLEMENTDATE) |
| `region` | str | NEM region ID (e.g. VIC1) |
| `rrp` | float | Regional Reference Price ($/MWh) |

### SCADA Parquet schema
| Column | Type | Description |
|--------|------|-------------|
| `DUID` | str | Dispatch Unit ID |
| `ts_start_nem` | datetime[tz=+10:00] | Start of 5-min interval |
| `ts_end_nem` | datetime[tz=+10:00] | End of interval |
| `mw` | float | SCADA reading (MW output) |

### DUID mapping CSV schema
| Column | Description |
|--------|-------------|
| `DUID` | Dispatch Unit ID (uppercase) |
| `Station` | Power station name |
| `Fuel` | Fuel category label |
| `Region` | NEM region (e.g. VIC1) |
