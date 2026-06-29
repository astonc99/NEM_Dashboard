# NEM Concepts — Training Guide

A plain-English explanation of the electricity market concepts used in this dashboard.
Useful context if you're presenting this as a portfolio project.

---

## What is the NEM?

The **National Electricity Market (NEM)** is Australia's wholesale electricity market, operating across Queensland, New South Wales, Victoria, South Australia, and Tasmania (not WA or NT — they have separate markets).

It is run by **AEMO** (Australian Energy Market Operator), which coordinates dispatch, maintains system security, and publishes market data.

The NEM is a **real-time energy-only market**: generators are paid the spot price for every MWh they produce, and there are no long-term capacity payments (unlike many other markets).

---

## 5-minute dispatch

The NEM dispatches electricity every **5 minutes**. Every 5 minutes:
1. Generators submit offers (how much they'll produce at what price)
2. AEMO's dispatch engine (NEMDE) stacks these offers from cheapest to most expensive
3. The cheapest generators that collectively meet demand are dispatched
4. The price of the last (most expensive) unit needed sets the spot price for the interval

This is called **merit-order dispatch**. In practice, large coal and gas plants set the floor, gas peakers set the marginal price during high-demand periods, and batteries/interconnectors provide rapid response.

**SETTLEMENTDATE** in AEMO data is the *end* of the 5-minute interval — so a record stamped `14:35:00` covers the interval from `14:30:00` to `14:35:00`.

---

## Regional Reference Price (RRP)

The **RRP** is the spot price in $/MWh for a given NEM region and 5-minute interval. It is the price at which all dispatched generators in that region are paid (and all loads pay) for that interval.

Key price thresholds:
- **$0/MWh** — negative prices occur when there's more supply than demand (common in VIC on sunny days due to rooftop solar + large solar farms)
- **$300/MWh** — often a trigger threshold for demand response
- **$15,500/MWh** — Market Price Cap (MPC), the maximum allowed spot price per interval
- **-$1,000/MWh** — Market Floor Price (MFP), the minimum

**Why does VIC get so many negative prices?**
Victoria has a large amount of rooftop solar (not visible in SCADA — it sits behind the meter) plus utility solar and wind. On sunny, windy days, scheduled generation may need to go negative-price to force some of it offline, because it's cheaper to pay to keep large coal units running at minimum output than to restart them.

---

## DUIDs and stations

A **DUID** (Dispatch Unit ID) is the unique identifier for a single dispatchable unit registered with AEMO. One power station often has multiple DUIDs:

- **Loy Yang A** has DUIDs: LYA1, LYA2, LYA3, LYA4 (one per generating unit)
- **Loy Yang B** has DUIDs: LOYYB1, LOYYB2

Batteries are split into two DUIDs per physical battery:
- `BESS1G1` — generator mode (discharging, injecting MW)
- `BESS1L1` — load mode (charging, consuming MW)

This is why the DUID mapping has G/L variant resolution — we look up the base DUID for fuel type.

**SCADAVALUE** is the real-time MW reading from the SCADA system for that unit. Positive = generating, negative (rare in dispatch data) = consuming.

---

## Fuel types in the NEM

| Fuel | Technology | Role in NEM |
|------|------------|-------------|
| **Brown Coal** | Steam turbine | Baseload — low cost, slow to ramp. VIC-specific (Latrobe Valley). |
| **Black Coal** | Steam turbine | Baseload — mainly NSW and QLD. |
| **Gas - CCGT** | Combined cycle gas turbine | Mid-merit — more efficient than OCGT, slower to ramp |
| **Gas - OCGT/Peaker** | Open cycle gas turbine | Peaker — fast start, expensive, used for high-demand periods |
| **Gas - Other** | Steam turbine, reciprocating | Various |
| **Hydro** | Various | Dispatchable renewable — fast ramp, used for frequency control and peak demand |
| **Wind** | Wind turbine | Non-dispatchable (follows wind), large and growing share |
| **Solar - Utility** | Photovoltaic | Non-dispatchable (follows sun), growing rapidly |
| **Battery** | Li-ion BESS | Fast response, price arbitrage (charge cheap, discharge expensive) |
| **Bioenergy** | Biomass/biogas combustion | Small but dispatchable renewable |
| **Interconnector** | Transmission line | Imports/exports between regions — treated as a virtual generator |

---

## Interconnectors

NEM regions are connected by high-voltage transmission links called **interconnectors**:
- **Heywood** — VIC to SA
- **Murraylink** — VIC to SA (DC cable)
- **Basslink** — VIC to TAS (undersea DC cable)
- **VIC-NSW** — VIC to NSW (multiple circuits)
- **QNI** — QLD to NSW

When VIC has surplus renewable generation, it typically exports north to NSW or SA. When VIC demand is high (cold nights), it imports from NSW. Interconnector flow data comes from the `DISPATCHINTERCONNECTORRES` MMSDM table (not yet in this dashboard — see roadmap).

---

## Renewable penetration

**Renewable penetration** = (Wind + Solar + Hydro + Bioenergy) ÷ Total scheduled generation × 100%

Important caveat: this dashboard only sees **scheduled** (NEM-registered) generation. Rooftop solar is behind-the-meter and reduces grid demand rather than appearing as generation. So when rooftop solar is high, total SCADA MW goes down and the "demand" falls — you can see this as a midday dip in generation (the "duck curve").

VIC is one of the highest penetration regions in the world on a percentage basis. 100%+ renewable penetration is now common on sunny weekends.

---

## Price duration curve

A **price duration curve** answers the question: *"What price is exceeded X% of the time?"*

- X-axis: % of time the price is at or above this level (0% = maximum price, 100% = minimum price)
- Y-axis: price ($/MWh)

Reading the curve:
- Where it crosses $0 on the Y-axis → that X% tells you how often prices are negative
- The 50th percentile (X=50%) is the median price
- A steep left side means occasional very high spikes
- A flat middle section means a stable mid-merit price band

VIC's curve has become much flatter and lower in recent years due to renewables, with a larger negative-price region on the left.

---

## MMSDM data

**MMSDM** (Market Management System Data Model) is AEMO's data warehouse for historical market data. It covers everything from dispatch prices to bidding data to constraint equations.

Key tables used in this dashboard:
- `DISPATCH_PRICE` — 5-minute RRP for all regions
- `DISPATCH_UNIT_SCADA` — 5-minute MW reading for every registered generating unit

MMSDM data is published monthly, approximately 6 weeks after the data month ends, as ZIP files at:
`nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/`

For data within the last 6 weeks, we use **NEMWeb** instead:
`nemweb.com.au/Reports/Archive/` — rolling archive of individual 5-minute interval files

---

## What "backfill" means in this dashboard

The dashboard stores data locally in Parquet files (one file per month per data type). "Backfill" means downloading a historical month from AEMO and adding it to the local store.

**MMSDM backfill** → one ZIP download per month, fast (seconds)
**NEMWeb sync** → hundreds of individual file downloads per month, parallelised but slower (minutes for a full month)

The **Sync to Today** button automatically chooses the right source for each missing month.
