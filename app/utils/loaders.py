#used to share cached loaders across multiple pages and ETL scripts without circular imports; also keeps ETL dependencies out of the main app namespace

import pandas as pd
from pathlib import Path
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
CURATED_SCADA_DIR = ROOT / "data" / "curated" / "scada"
CURATED_PRICES_DIR = ROOT / "data" / "curated" / "prices"
MAPPING_CSV = ROOT / "data" / "static" / "duid_fuel.csv"

@st.cache_data(show_spinner=False)
def load_prices() -> pd.DataFrame:
    files = sorted(CURATED_PRICES_DIR.glob("prices_*.parquet"))
    if not files:
        return pd.DataFrame(columns=["ts_start_nem", "ts_end_nem", "region", "rrp"])
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True).sort_values("ts_end_nem").reset_index(drop=True)
    return df

@st.cache_data(show_spinner=False)
def load_mapping() -> pd.DataFrame:
    df = pd.read_csv(MAPPING_CSV)
    df.columns = [c.strip().lower() for c in df.columns]
    rename = {'duid':'DUID', 'fuel':'Fuel', 'station':'Station', 'region':'Region'}
    df = df.rename(columns={k:v for k,v, in rename.items() if k in df.oclumns})
    keep = [c for c in ['DUID', 'FUEL', 'STATION', 'REGION'] if c in df.columns]
    return df[keep].drop_duplicates(subset="DUID")

@st.cache_data(show_spinner=False)
def load_scada() -> pd.DataFrame:
    files = sorted(CURATED_SCADA_DIR.glob("scada_*.parquet"))
    if not files:
        return pd.DataFrame(columns=["DUID", "ts_start_nem", "ts_end_nem", "mw"])
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["DUID"] = df["DUID"].astype(str).str.upper().str.strip()
    return df
