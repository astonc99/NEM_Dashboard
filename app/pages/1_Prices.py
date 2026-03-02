import streamlit as st
import pandas as pd
from pathlib import Path
import sys
import datetime as dt
from utils.loaders import load_prices, load_scada, load_mapping

# allow imports from ../etl
sys.path.append(str(Path(__file__).resolve().parents[2] / "etl"))
from mmsdm_price import backfill_one

CURATED_PRICES_DIR = Path(__file__).resolve().parents[1] / "data" / "curated" / "prices"

st.set_page_config(page_title="NEM VIC – Prices", layout="wide")
st.title("NEM Victoria – 5‑minute Prices (Live sample)")

with st.sidebar:
    st.header("Controls")
    st.subheader("Backfill month (MMSDM)")
    y = st.number_input("Year", min_value=2024, max_value= dt.date.today().year, value = 2025, step = 1)
    m = st.number_input("Month", min_value = 1, max_value = 12, value =12, step = 1)
    if st.button("Backfill"):
        p = backfill_one(int(y),int(m))
        st.success(f"Backfilled: {p.name}")
        st.cache_data.clear()

@st.cache_data(show_spinner=False)
def load_all_prices() -> pd.DataFrame:
    files = sorted(CURATED_PRICES_DIR.glob("prices_*.parquet"))
    if not files:
            return pd.DataFrame(columns=["ts_start_nem", "ts_end_nem", "region", "rrp"])
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True).sort_values("ts_end_nem")
    return df

df = load_all_prices()

if df.empty:
    st.info("No Data yet. Backfill a month using the sidebar")
    st.stop()



col1, col2, col3 = st.columns(3)
latest_price = df["rrp"].iloc[-1]
today_mask = df["ts_end_nem"].dt.date == df["ts_end_nem"].iloc[-1].date()
today_avg = df.loc[today_mask, "rrp"].mean()
neg_count = (df.loc[today_mask, "rrp"] < 0).sum()

col1.metric("Latest RRP (A$/MWh)", f"{latest_price:,.2f}")
col2.metric("Today's Avg RRP", f"{today_avg:,.2f}")
col3.metric("Negative Price Intervals (today)", f"{int(neg_count)}")

min_date = df["ts_end_nem"].min().date()
max_date = df["ts_end_nem"].max().date()
start, end = st.date_input("Display range", (min_date, max_date), min_value=min_date, max_value=max_date)


mask = (df["ts_end_nem"].dt.date >= start) & (df["ts_end_nem"].dt.date <= end)
plot_df = df.loc[mask]

st.line_chart(plot_df.set_index("ts_end_nem")["rrp"], height=320, use_container_width=True)
st.dataframe(plot_df.tail(100))
