from fastapi import APIRouter, Query
from pathlib import Path
from datetime import date
from typing import Optional
import pandas as pd

router = APIRouter()

PRICES_DIR  = Path("data/curated/prices")
SCADA_DIR   = Path("data/curated/scada")
MAPPING_CSV = Path("data/static/duid_fuel.csv")

RENEWABLE_FUELS = {"Wind", "Solar - Utility", "Hydro", "Bioenergy"}


def _load_prices() -> pd.DataFrame:
    files = sorted(PRICES_DIR.glob("prices_*.parquet"))
    if not files:
        return pd.DataFrame(columns=["ts_end_nem", "region", "rrp"])
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _load_scada() -> pd.DataFrame:
    files = sorted(SCADA_DIR.glob("scada_*.parquet"))
    if not files:
        return pd.DataFrame(columns=["DUID", "ts_end_nem", "mw"])
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _load_mapping(region: str) -> pd.DataFrame:
    if not MAPPING_CSV.exists():
        return pd.DataFrame(columns=["DUID", "Fuel", "Region"])
    df = pd.read_csv(MAPPING_CSV, dtype=str)
    df["DUID"] = df["DUID"].str.strip().str.upper()
    return df[df["Region"].str.upper() == region.upper()]


@router.get("/renewable-penetration")
def get_renewable_penetration(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    region: str = Query("VIC1"),
    resample: str = Query("30min"),
):
    scada = _load_scada()
    if scada.empty:
        return {"data": [], "avg_pct": None}

    mapping = _load_mapping(region)
    df = scada.merge(mapping[["DUID", "Fuel"]], on="DUID", how="inner")

    if start_date:
        df = df[df["ts_end_nem"].dt.date >= start_date]
    if end_date:
        df = df[df["ts_end_nem"].dt.date <= end_date]

    if df.empty:
        return {"data": [], "avg_pct": None}

    df = df.set_index("ts_end_nem").sort_index()
    df["is_renewable"] = df["Fuel"].isin(RENEWABLE_FUELS)

    total = df["mw"].resample(resample).sum()
    renewable = df.loc[df["is_renewable"], "mw"].resample(resample).sum()

    result = pd.DataFrame({"total_mw": total, "renewable_mw": renewable}).fillna(0)
    result["pct"] = (result["renewable_mw"] / result["total_mw"].replace(0, float("nan")) * 100).round(1).fillna(0)
    result = result.reset_index()

    avg_pct = round(float(result["pct"].mean()), 1) if not result.empty else None

    data = [
        {
            "ts": row["ts_end_nem"].strftime("%Y-%m-%dT%H:%M:%S+10:00"),
            "total_mw": round(float(row["total_mw"]), 1),
            "renewable_mw": round(float(row["renewable_mw"]), 1),
            "other_mw": round(float(row["total_mw"] - row["renewable_mw"]), 1),
            "pct": float(row["pct"]),
        }
        for _, row in result.iterrows()
    ]

    return {"data": data, "avg_pct": avg_pct}


@router.get("/price-duration")
def get_price_duration(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    region: str = Query("VIC1"),
):
    df = _load_prices()
    if df.empty:
        return {"data": [], "pct_negative": None, "median_rrp": None}

    df = df[df["region"] == region]
    if start_date:
        df = df[df["ts_end_nem"].dt.date >= start_date]
    if end_date:
        df = df[df["ts_end_nem"].dt.date <= end_date]

    if df.empty:
        return {"data": [], "pct_negative": None, "median_rrp": None}

    sorted_rrp = df["rrp"].sort_values(ascending=False).reset_index(drop=True)
    n = len(sorted_rrp)

    # Sample down to ~2000 points for chart performance
    step = max(1, n // 2000)
    indices = list(range(0, n, step))
    if indices[-1] != n - 1:
        indices.append(n - 1)

    data = [
        {
            "pct_time": round(i / (n - 1) * 100, 2),
            "rrp": round(float(sorted_rrp.iloc[i]), 2),
        }
        for i in indices
    ]

    pct_negative = round(float((df["rrp"] < 0).sum() / n * 100), 1)
    median_rrp = round(float(df["rrp"].median()), 2)

    return {
        "data": data,
        "pct_negative": pct_negative,
        "median_rrp": median_rrp,
        "total_intervals": n,
    }
