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
SCADA_DIR = Path("data/curated/scada")
FUEL_MAP = Path("data/static/duid_fuel.csv")

FUEL_ORDER = [
    "Black Coal", "Brown Coal",
    "Gas - CCGT", "Gas - OCGT/Peaker", "Gas - Other",
    "Hydro", "Wind", "Solar - Utility",
    "Battery", "Bioenergy", "Liquid Fuel",
    "Demand Response", "Interconnector", "Other",
]

FUEL_COLORS = {
    "Black Coal":      "#1f2937",
    "Brown Coal":      "#78350f",
    "Gas - CCGT":      "#ea580c",
    "Gas - OCGT/Peaker": "#fb923c",
    "Gas - Other":     "#fdba74",
    "Hydro":           "#3b82f6",
    "Wind":            "#06b6d4",
    "Solar - Utility": "#fbbf24",
    "Battery":         "#8b5cf6",
    "Bioenergy":       "#22c55e",
    "Liquid Fuel":     "#ef4444",
    "Demand Response": "#ec4899",
    "Interconnector":  "#64748b",
    "Other":           "#374151",
}


def _load_mapping(region: str = "VIC1") -> pd.DataFrame:
    if not FUEL_MAP.exists():
        return pd.DataFrame(columns=["DUID", "Station", "Fuel", "Region"])
    df = pd.read_csv(FUEL_MAP, comment="#")
    df["DUID"] = df["DUID"].str.strip().str.upper()

    # Build a base-DUID lookup to resolve battery G/L variant DUIDs.
    # AEMO now registers battery charge/discharge as separate DUIDs, e.g.
    # BALB1 → BALBG1 (generator/discharge) and BALBL1 (load/charge).
    # These don't appear in the registration workbook, so we derive them.
    base_lookup = (
        df.drop_duplicates(subset=["DUID"], keep="last")
        .set_index("DUID")[["Station", "Fuel", "Region"]]
        .to_dict("index")
    )

    def _resolve(duid: str):
        import re
        # Strip trailing G<n> or L<n> suffix and look up the base DUID
        base = re.sub(r"[GL]\d+$", "", duid)
        if base != duid and base in base_lookup:
            row = base_lookup[base]
            return {"DUID": duid, "Station": row["Station"], "Fuel": row["Fuel"], "Region": row["Region"]}
        # AEMO virtual distributed-generation aggregates
        if duid.startswith("DG_"):
            rgn = duid.split("_", 1)[-1] if "_" in duid else ""
            return {"DUID": duid, "Station": "Distributed Generation", "Fuel": "Solar - Utility", "Region": rgn}
        # Demand Response Exchange virtual units
        if duid.startswith("DRX"):
            return {"DUID": duid, "Station": "Demand Response", "Fuel": "Demand Response", "Region": ""}
        return None

    # Collect known DUIDs then add synthetic rows for the unrecognised patterns
    known = set(df["DUID"])
    all_scada_duids: list[str] = []
    if SCADA_DIR.exists():
        seen: set[str] = set()
        for f in sorted(SCADA_DIR.glob("scada_*.parquet")):
            chunk = pd.read_parquet(f, columns=["DUID"])["DUID"].str.upper().str.strip().unique()
            for d in chunk:
                if d not in known and d not in seen:
                    seen.add(d)
                    all_scada_duids.append(d)

    extras = [r for d in all_scada_duids if (r := _resolve(d)) is not None]
    if extras:
        df = pd.concat([df, pd.DataFrame(extras)], ignore_index=True)

    if region:
        df = df[df["Region"] == region]
    return df


def _load_all_scada() -> pd.DataFrame:
    files = sorted(SCADA_DIR.glob("scada_*.parquet"))
    if not files:
        return pd.DataFrame(columns=["DUID", "ts_start_nem", "ts_end_nem", "mw"])
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True).sort_values("ts_end_nem").reset_index(drop=True)


@router.get("/")
def get_generation(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    region: str = Query("VIC1"),
):
    scada = _load_all_scada()
    if scada.empty:
        return {"data": [], "fuels": [], "fuel_colors": {}, "meta": {}}

    mapping = _load_mapping(region)
    df = scada.merge(mapping[["DUID", "Fuel"]], on="DUID", how="inner")
    df["Fuel"] = df["Fuel"].fillna("Other")

    if start_date:
        df = df[df["ts_end_nem"].dt.date >= start_date]
    if end_date:
        df = df[df["ts_end_nem"].dt.date <= end_date]

    if df.empty:
        return {"data": [], "fuels": [], "fuel_colors": {}, "meta": {}}

    agg = df.groupby(["ts_end_nem", "Fuel"])["mw"].sum().reset_index()
    pivot = (
        agg.pivot_table(index="ts_end_nem", columns="Fuel", values="mw", fill_value=0)
        .reset_index()
    )

    fuels_present = [f for f in FUEL_ORDER if f in pivot.columns]
    other_fuels = sorted(c for c in pivot.columns if c not in FUEL_ORDER and c != "ts_end_nem")
    all_fuels = fuels_present + other_fuels

    records = []
    for _, row in pivot.iterrows():
        rec = {"ts": row["ts_end_nem"].strftime("%Y-%m-%dT%H:%M:%S+10:00")}
        for fuel in all_fuels:
            if fuel in row.index:
                rec[fuel] = round(float(row[fuel]), 1)
        records.append(rec)

    return {
        "data": records,
        "fuels": all_fuels,
        "fuel_colors": {f: FUEL_COLORS.get(f, "#94A3B8") for f in all_fuels},
        "meta": {
            "min_date": str(df["ts_end_nem"].min().date()),
            "max_date": str(df["ts_end_nem"].max().date()),
            "duid_count": int(df["DUID"].nunique()),
            "fuel_count": int(df["Fuel"].nunique()),
        },
    }


@router.get("/top-units")
def get_top_units(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    region: str = Query("VIC1"),
    limit: int = Query(20),
):
    scada = _load_all_scada()
    if scada.empty:
        return {"data": []}

    mapping = _load_mapping(region)
    df = scada.merge(mapping[["DUID", "Fuel", "Station"]], on="DUID", how="inner")

    if start_date:
        df = df[df["ts_end_nem"].dt.date >= start_date]
    if end_date:
        df = df[df["ts_end_nem"].dt.date <= end_date]

    if df.empty:
        return {"data": []}

    top = (
        df.groupby(["DUID", "Fuel", "Station"])["mw"]
        .agg(avg_mw="mean", energy_mwh=lambda x: round(x.sum() * 5 / 60, 1), intervals="count")
        .reset_index()
        .sort_values("avg_mw", ascending=False)
        .head(limit)
    )
    top["avg_mw"] = top["avg_mw"].round(1)
    return {"data": top.to_dict(orient="records")}


@router.get("/meta")
def get_meta(region: str = Query("VIC1")):
    scada = _load_all_scada()
    if scada.empty:
        return {"min_date": None, "max_date": None, "duid_count": 0}
    mapping = _load_mapping(region)
    df = scada.merge(mapping[["DUID"]], on="DUID", how="inner")
    if df.empty:
        return {"min_date": None, "max_date": None, "duid_count": 0}
    return {
        "min_date": str(df["ts_end_nem"].min().date()),
        "max_date": str(df["ts_end_nem"].max().date()),
        "duid_count": int(df["DUID"].nunique()),
    }


@router.post("/backfill")
def backfill_generation(req: BackfillRequest):
    try:
        from etl.mmsdm_scada import backfill_one
        path = backfill_one(req.year, req.month)
        return {"status": "ok", "path": str(path), "year": req.year, "month": req.month}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/sync")
def sync_generation(req: SyncRequest = None):
    try:
        from etl.auto_sync import sync_scada as _sync
        r = req or SyncRequest()
        return _sync(start_date=r.start_date, end_date=r.end_date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
