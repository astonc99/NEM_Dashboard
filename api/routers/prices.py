from fastapi import APIRouter, Query, HTTPException
from pathlib import Path
from datetime import date
from typing import Optional
import pandas as pd
from pydantic import BaseModel


class BackfillRequest(BaseModel):
    year: int
    month: int


class SyncRequest(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None

router = APIRouter()
PRICES_DIR = Path("data/curated/prices")


def _load_all_prices() -> pd.DataFrame:
    files = sorted(PRICES_DIR.glob("prices_*.parquet"))
    if not files:
        return pd.DataFrame(columns=["ts_start_nem", "ts_end_nem", "region", "rrp"])
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    return df.sort_values("ts_end_nem").reset_index(drop=True)


@router.get("/")
def get_prices(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    region: str = Query("VIC1"),
):
    df = _load_all_prices()
    if df.empty:
        return {"data": [], "meta": {"min_date": None, "max_date": None, "count": 0}}

    df = df[df["region"] == region]
    if start_date:
        df = df[df["ts_end_nem"].dt.date >= start_date]
    if end_date:
        df = df[df["ts_end_nem"].dt.date <= end_date]

    records = [
        {
            "ts": row["ts_end_nem"].strftime("%Y-%m-%dT%H:%M:%S+10:00"),
            "rrp": round(float(row["rrp"]), 2),
        }
        for _, row in df[["ts_end_nem", "rrp"]].iterrows()
    ]

    meta = {
        "min_date": str(df["ts_end_nem"].min().date()) if not df.empty else None,
        "max_date": str(df["ts_end_nem"].max().date()) if not df.empty else None,
        "count": len(df),
    }
    return {"data": records, "meta": meta}


@router.get("/summary")
def get_summary(region: str = Query("VIC1")):
    df = _load_all_prices()
    empty = {"latest_rrp": None, "today_avg_rrp": None, "negative_intervals_today": 0, "latest_date": None}
    if df.empty:
        return empty

    df = df[df["region"] == region]
    if df.empty:
        return empty

    latest_date = df["ts_end_nem"].max().date()
    today = df[df["ts_end_nem"].dt.date == latest_date]

    return {
        "latest_rrp": round(float(df["rrp"].iloc[-1]), 2),
        "today_avg_rrp": round(float(today["rrp"].mean()), 2),
        "negative_intervals_today": int((today["rrp"] < 0).sum()),
        "latest_date": str(latest_date),
    }


@router.get("/meta")
def get_meta(region: str = Query("VIC1")):
    df = _load_all_prices()
    if df.empty:
        return {"min_date": None, "max_date": None, "count": 0}
    df = df[df["region"] == region]
    if df.empty:
        return {"min_date": None, "max_date": None, "count": 0}
    return {
        "min_date": str(df["ts_end_nem"].min().date()),
        "max_date": str(df["ts_end_nem"].max().date()),
        "count": len(df),
    }


@router.post("/backfill")
def backfill_prices(req: BackfillRequest):
    try:
        from etl.mmsdm_price import backfill_one
        path = backfill_one(req.year, req.month)
        return {"status": "ok", "path": str(path), "year": req.year, "month": req.month}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/sync")
def sync_prices(req: SyncRequest = None):
    try:
        from etl.auto_sync import sync_prices as _sync
        r = req or SyncRequest()
        return _sync(start_date=r.start_date, end_date=r.end_date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
