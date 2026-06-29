# Getting Started

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11+ | ETL pipeline and API backend |
| Node.js | 18+ | React frontend |
| Git | any | Version control |

## First-time setup

### 1. Python environment
```bash
# From the project root
python -m venv .venv
.venv\Scripts\activate       # Windows
pip install -r requirements.txt
```

### 2. Frontend dependencies
```bash
cd frontend
npm install
cd ..
```

### 3. DUID fuel mapping
The generation charts need a mapping from generator unit IDs to fuel types.
```bash
python etl/build_duid_mapping.py
```
This downloads the AEMO Registration & Exemption List workbook, classifies every DUID by fuel type, and writes `data/static/duid_fuel.csv`. It takes ~30 seconds.

### 4. Get some data
The dashboard needs at least one month of price and SCADA data. The easiest way is to start the app and use the **Sync to Today** button, or run:
```bash
# Get one specific month (e.g. July 2024)
python -c "from etl.mmsdm_price import backfill_one; backfill_one(2024, 7)"
python -c "from etl.mmsdm_scada import backfill_one; backfill_one(2024, 7)"
```
MMSDM data is available for any month from ~2009 onwards, as long as it ended more than 6 weeks ago.

### 5. Run the app
```
start.bat
```
This opens two terminal windows — one for the API (port 8000) and one for the frontend (port 5173). Open `http://localhost:5173` in your browser.

---

## Syncing data

### Sync to Today (recommended)
Each page has a **Sync to Today** button. Click it and the app will:
1. Find the latest date stored in your Parquet files
2. For each missing month where MMSDM has published data (>6 weeks old): download the monthly ZIP
3. For the current and very recent months: download individual 5-minute interval files from NEMWeb Archive
4. Cap at 12 months per click — if you have a large gap, click again after it finishes

### Manual MMSDM backfill (specific month)
The old BackfillPanel is still available via API. To fetch a specific month:
```bash
# Using curl
curl -X POST http://localhost:8000/api/prices/backfill \
  -H "Content-Type: application/json" \
  -d '{"year": 2024, "month": 7}'
```

### Rebuild DUID mapping
If AEMO updates their registration workbook (a few times per year):
```bash
python etl/build_duid_mapping.py
```
Manual overrides in `MANUAL_OVERRIDES` dict survive every re-run.

---

## Common issues

**"No generation data for this range"**
The selected date range has no SCADA parquet files. Use Sync to Today or the MMSDM backfill to add data for that period.

**"No price data available"**
Same issue for prices. Check `data/curated/prices/` — if empty, run a backfill first.

**API won't start — ModuleNotFoundError**
Make sure you're running uvicorn from the project root, not from inside `api/`. The `start.bat` handles this automatically.

**Frontend can't reach the API**
Vite proxies `/api` → `http://localhost:8000`. Make sure the API is running before opening the frontend. Both should be started by `start.bat`.

**DUIDs showing as "Other" fuel type**
Re-run `python etl/build_duid_mapping.py`. The MANUAL_OVERRIDES in that file cover the known VIC1 coal and gas units.
