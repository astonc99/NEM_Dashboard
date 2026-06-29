# Roadmap — Future Improvements

Grouped by effort and value. Items at the top of each section are the most achievable next steps.

---

## Near-term (existing data, straightforward to add)

### All NEM regions
Currently the dashboard is locked to VIC1. The data already contains all regions — it's a matter of:
1. Adding a region selector dropdown to each page
2. Passing the selected region to API calls
3. Updating DUID mapping display to filter by region

**Value:** Makes the dashboard useful for NSW, QLD, SA, TAS analysts — much broader appeal.

### Negative price frequency trend
Monthly bar chart: how many 5-min intervals per month had RRP < $0.
- Data: already in price Parquet files
- Chart: Recharts BarChart, one bar per month, colour coded by count
- **Value:** Tells the renewable oversupply story clearly. VIC is world-leading here.

### Price spike table
Count of intervals above thresholds ($300, $1,000, $5,000/MWh) per month.
- Data: already in price Parquet files
- Useful for showing tail-risk and fuel scarcity events
- **Value:** Common metric in energy market analysis

### Battery dispatch pattern
Show battery G (discharge) and L (load) separately on the generation chart.
- Data: already in SCADA files, DUIDs already split by G/L variant
- **Value:** Visually shows price arbitrage — batteries charge during negative-price periods and discharge during spikes

---

## Medium-term (new data sources or moderate complexity)

### Interconnector flows
VIC imports/exports to NSW, SA, TAS over time.
- Data source: `DISPATCHINTERCONNECTORRES` table in MMSDM (not yet downloaded)
- New ETL: `etl/mmsdm_interconnector.py`
- New API endpoint: `GET /api/interconnector/`
- Chart: Multi-line chart or stacked bar showing net flows by interconnector
- **Value:** Shows VIC's role as renewable exporter on high-renewable days

### Carbon intensity
Estimated tCO2/MWh from fuel mix at each interval.
- Requires emission factor table (publicly available from Clean Energy Regulator)
- Calculation: sum(MW × emission_factor) / total_MW for each interval
- **Value:** High-impact visual metric for renewable story

### Demand vs supply
Show scheduled generation vs operational demand (from `DISPATCHREGIONSUM` MMSDM table).
- Useful for showing the "duck curve" (midday demand suppression from rooftop solar)
- **Value:** Explains why negative prices occur and the role of dispatchable storage

### Automated scheduled sync
Currently sync is triggered manually. Add a Windows Task Scheduler task or a simple scheduler:
- Run `POST /api/prices/sync` and `POST /api/generation/sync` at e.g. 06:00 daily
- Could also add a `GET /api/status` endpoint showing last sync timestamp
- **Value:** Dashboard stays current without manual intervention

---

## Longer-term (significant new features)

### Price forecasting
Two practical approaches for the NEM:

**1. Statistical / ML forecasting**
- Input features: lagged prices (t-1, t-2, ...), hour of day, day of week, temperature, scheduled renewable output
- Models: LSTM (good for sequences), LightGBM/XGBoost (good for tabular, faster to train)
- Output: predicted RRP for the next 1-48 hours
- Implementation: train offline in Python (scikit-learn / PyTorch), expose predictions via `GET /api/forecast/prices`
- **How it works:** The model learns that e.g. "midday + high wind forecast + low demand = likely negative price" from historical patterns

**2. Merit-order simulation**
- More interpretable: reconstruct the supply stack from DUID bids (from MMSDM `BIDDAYOFFER` / `BIDPEROFFER` tables)
- Simulate where the demand curve intersects the supply stack
- More accurate in structural terms but requires much more data
- Better suited to dispatch-level analysis

### Multi-region comparison
Side-by-side RRP for all five regions on one chart. Shows:
- Price differentials between regions (indicates congestion)
- How interconnectors equalise prices when unconstrained
- Regional renewable profiles (QLD solar afternoon, SA wind nights)

### Forecasting UI
- Show forecast alongside actuals on the price chart
- Confidence intervals as shaded area
- Model accuracy metrics (MAE, RMSE, directional accuracy)

### WebSocket live data
Replace the manual "Sync to Today" with a background WebSocket connection:
- Backend: FastAPI WebSocket endpoint that polls NEMWeb Current every 5 minutes
- Frontend: live-updating chart (no page refresh needed)
- **Complexity:** Requires background task management and reconnection logic

### FCAS (Frequency Control Ancillary Services)
The NEM has a secondary market for frequency regulation services.
- Data: `DISPATCHLOAD` and `FCAS` tables in MMSDM
- Shows battery and hydro units' role in grid stability
- Highly relevant to energy storage analysis

---

## Portfolio presentation tips

The most interview-relevant things to highlight from this project:

1. **Data pipeline design** — two-tier ETL (MMSDM vs NEMWeb), smart source selection, Parquet columnar storage
2. **NEM domain knowledge** — DUIDs, SETTLEMENTDATE semantics, SQL Loader format, merit-order dispatch
3. **Full-stack delivery** — FastAPI → Parquet → React, all working together with one command
4. **Analytics thinking** — price duration curve and renewable penetration are metrics used by professional traders and analysts
5. **Forecasting (if built)** — ML applied to energy markets is a direct skill match for analyst roles at AEMO, AGL, Origin, EnergyAustralia, Macquarie, etc.
