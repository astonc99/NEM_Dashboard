# Generation mix graph adder

import streamlit as st
import pandas as pd
from pathlib import Path
import sys
import datetime as dt
from utils.loaders import load_prices, load_scada, load_mapping
#Add etl pto path
sys.path.append(str(Path(__file__).resolve().parents[1] / "etl"))

#add data links directories to file

CURATED_SCADA_DIR = Path(__file__).resolve().parents[1] / "data" / "curated" / "scada"
MAPPING_CSV = Path(__file__).resolve().parents[1] / "data" / "static" / "duid_fuel.csv"

st.set_page_config(page_title="NEM VIC – Generation Mix", layout="wide")
st.title("NEM Victoria – Generation Mix (Live sample)")

#Cached Loader DUID -> fuel mapping
@st.cache_data(show_spinner=False)

def load_mapping() -> pd.DataFrame:
    df = pd.read_csv(MAPPING_CSV)
    df.columns = [c.strip() for c in df.columns]
    rename = {'duid':'DUID', 'fuel':'Fuel', 'station':'Station', 'region':'Region'}
    df = df.rename(columns={k:v for k,v in rename.items() if k in df.columns})

    if "Fuel" in df.columns:
        df["Fuel"] = (df["Fuel"].astype(str).str.strip().str.title().replace({"Black Coal":'Coal','Brown Coal':'Coal','Solar PV':'Solar','Solar Farm':'Solar','Battery Charging':'Battery','Battery Discharging':'Battery'}))  
        keep = [c for c in ["DUID","Fuel","Station","Region"] if c in df.columns]
        df = df[keep].drop_duplicates(subset=["DUID"])
        return df
    
@st.cache_data(show_spinner=False)
def load_scada_data() -> pd.DataFrame:
    files = sorted(CURATED_SCADA_DIR.glob("scada_*.parquet"))
    if not files:
        return pd.DataFrame(columns=["DUID", "ts_start_nem", "ts_end_nem", "mw"])
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df.sort_values("ts_end_nem").reset_index(drop=True)
    return df

#read datasets
mapping = load_mapping()
scada = load_scada_data()   

if scada.empty:
    st.info("No SCADA data yet. Backfill a month using the Prices page")
    st.stop()
if "Region" not in mapping.columns:
    st.warning("Mapping missing 'Region' column. Region data will be unavailable.")

mapping_vic = mapping[mapping.get("Region","VIC1") == "VIC1"].copy()

scada = scada.merge(mapping_vic[['DUID','Fuel']], on="DUID", how="left")
scada["Fuel"] = scada["Fuel"].fillna("Other")

min_date = scada["ts_end_nem"].min().date()
max_date = scada["ts_end_nem"].max().date()
start_date, end_date = st.date_input("Display range", (max_date.replace(day=1), max_date), min_value=min_date, max_value=max_date)

mask = (scada['ts_end_nem'].dt.date >= start_date) & (scada['ts_end_nem'].dt.date <= end_date)
scada_sel = scada.loc[mask].copy()

if scada_sel.empty:
    st.info("No SCADA data in the selected date range.")
    st.stop()

st.caption(f"Intervals: {len(scada_sel):,} • DUIDs: {scada_sel['DUID'].nunique():,} • Fuels: {scada_sel['Fuel'].nunique():,}")

import altair as alt

# Aggregate by time & fuel
mix = (scada_sel
       .groupby(['ts_end_nem', 'Fuel'], as_index=False)['mw']
       .sum())

# Optional: stable fuel order + colors
fuel_order = ['Coal','Gas','Hydro','Wind','Solar','Battery','Bio','Other']
palette = ['#4B4B4B','#E07A5F','#3A86FF','#83C5BE','#FFD166','#8E44AD','#6A994E','#9E9E9E']

chart = (
    alt.Chart(mix)
    .mark_area(interpolate='step-after')
    .encode(
        x=alt.X('ts_end_nem:T', title='Time'),
        y=alt.Y('mw:Q', stack='zero', title='MW'),
        color=alt.Color('Fuel:N',
                        sort=fuel_order,
                        scale=alt.Scale(domain=fuel_order, range=palette),
                        legend=alt.Legend(title='Fuel')),
        tooltip=[
            alt.Tooltip('ts_end_nem:T', title='Interval'),
            alt.Tooltip('Fuel:N'),
            alt.Tooltip('mw:Q', title='MW', format=',.0f')
        ]
    )
    .properties(height=360)
)

st.subheader("Generation mix by fuel (MW)")
st.altair_chart(chart, use_container_width=True)

# Compute unit stats
duid_stats = (
    scada_sel
    .groupby(['DUID','Fuel'], as_index=False)
    .agg(
        avg_MW=('mw', 'mean'),
        energy_MWh=('mw', lambda s: s.sum() * 5/60),
        intervals=('mw', 'size')
    )
    .sort_values('avg_MW', ascending=False)
)

st.subheader("Top units by average MW")
st.dataframe(
    duid_stats.head(20)
             .assign(avg_MW=lambda d: d['avg_MW'].round(1),
                     energy_MWh=lambda d: d['energy_MWh'].round(0))
)

with st.sidebar:
    st.header("Data ops")
    st.caption("Run with care; clears caches.")
    y = st.number_input("Backfill SCADA year", min_value=2010, max_value=dt.date.today().year, value=2024, step=1)
    m = st.number_input("Backfill SCADA month", min_value=1, max_value=12, value=7, step=1)

    # Lazy imports so the page still runs if ETL modules are missing
    try:
        from mmsdm_scada import backfill_one as scada_backfill_one  # your function
    except Exception:
        scada_backfill_one = None

    # try mapping rebuild if you have a callable entrypoint in build_duid_mapping.py
    try:
        from build_duid_mapping import main as rebuild_duid_mapping
    except Exception:
        rebuild_duid_mapping = None

    if st.button("Backfill SCADA month"):
        if scada_backfill_one is None:
            st.error("ETL function mmsdm_scada.backfill_one not found.")
        else:
            p = scada_backfill_one(int(y), int(m))
            st.success(f"Backfilled: {Path(p).name}")
            st.cache_data.clear()

    if st.button("Refresh DUID mapping"):
        if rebuild_duid_mapping is None:
            st.error("build_duid_mapping.main not found.")
        else:
            rebuild_duid_mapping()
            st.success("Mapping refreshed.")
            st.cache_data.clear()