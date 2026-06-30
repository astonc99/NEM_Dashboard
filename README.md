# NEM Dashboard

A full-stack electricity market dashboard for the Australian National Electricity Market (NEM), built with live AEMO data. Portfolio project for energy market analytics roles.

## What it does

- Pulls 5-minute spot prices (RRP) and generator output (SCADA) from AEMO's public data feeds
- Serves the data via a REST API with automatic gap detection and backfill
- Visualises it in a dark-themed React app with interactive charts and a backfill panel

## Stack

| Layer | Tech |
|-------|------|
| Frontend | React + Vite, Recharts, Tailwind CSS |
| Backend | FastAPI (Python), Uvicorn |
| ETL | Python — AEMO MMSDM archive + NEMWeb live feed |
| Data | Parquet files (partitioned by month) |

## Pages

| Route | What you see |
|-------|-------------|
| `/` | Overview — KPI cards, quick links |
| `/prices` | Spot price time series, recent intervals table, backfill panel |
| `/generation` | Stacked fuel-type area chart, top dispatch units |
| `/analytics` | Renewable penetration curve, price duration curve |

## Quick start

```bash
# Install Python deps
pip install -r requirements.txt

# Install frontend deps
cd frontend && npm install && cd ..

# Start both servers (Windows)
start.bat
```

- API runs on [http://localhost:8000](http://localhost:8000)
- Frontend runs on [http://localhost:5173](http://localhost:5173)

## Getting data

Once running, use the **Backfill** panel on the Prices or Generation Mix page to download historical data. The ETL automatically picks the right source:

- **MMSDM archive** — for months older than ~6 weeks (bulk monthly ZIP from AEMO)
- **NEMWeb** — for recent months (parallel download of individual 5-min files)

You can also trigger sync via the API directly:

```bash
curl -X POST http://localhost:8000/api/prices/sync
curl -X POST http://localhost:8000/api/generation/sync
```

## API routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/prices/` | 5-min RRP time series, filterable by date + region |
| GET | `/api/prices/summary` | Latest RRP, day average, negative interval count |
| GET | `/api/prices/meta` | Stored date range and row count |
| POST | `/api/prices/sync` | Auto-detect gaps and sync to today |
| GET | `/api/generation/` | Fuel-stacked generation time series |
| GET | `/api/generation/top-units` | Top dispatching DUIDs for a date range |
| POST | `/api/generation/sync` | Auto-detect gaps and sync to today |
| GET | `/api/analytics/renewable-penetration` | Renewable % time series (30-min buckets) |
| GET | `/api/analytics/price-duration` | Sorted RRP for price duration curve |

## Data sources

- **AEMO MMSDM** — Monthly ZIP archives, available ~6 weeks after month end
- **AEMO NEMWeb** — Rolling archive of individual dispatch files (current + ~13 months)
- **AEMO Registration & Exemption List** — DUID → fuel type mapping workbook

All data is public and freely available from [aemo.com.au](https://www.aemo.com.au).

## Key NEM concepts

| Term | Meaning |
|------|---------|
| RRP | Regional Reference Price — the 5-min spot price in A$/MWh |
| SETTLEMENTDATE | AEMO timestamps the *end* of each 5-min dispatch interval |
| DUID | Dispatch Unit ID — unique identifier for each generator |
| VIC1 | Victoria region. Others: NSW1, QLD1, SA1, TAS1 |
| SCADAVALUE | MW output for a DUID at each 5-min interval |
