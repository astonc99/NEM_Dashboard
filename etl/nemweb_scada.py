"""
Fetch recent DISPATCH_UNIT_SCADA from NEMWeb Archive/Current.
Used for months not yet published in MMSDM (roughly the last 6 weeks).
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
import io, re, zipfile
import requests
import pandas as pd
from dateutil import tz

ARCHIVE_URL = "https://nemweb.com.au/Reports/Archive/Dispatch_SCADA/"
CURRENT_URL = "https://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
CURATED = Path(__file__).resolve().parents[1] / "data" / "curated" / "scada"
CURATED.mkdir(parents=True, exist_ok=True)
TZ_NEM = tz.gettz("Etc/GMT-10")

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Python/NEM-Dashboard)"}


def _list_zip_urls(base_url: str) -> list[str]:
    r = requests.get(base_url, timeout=30, headers=_HEADERS)
    r.raise_for_status()
    hrefs = re.findall(r'href="([^"]+\.zip)"', r.text, re.IGNORECASE)
    out = []
    for h in hrefs:
        if h.startswith("http"):
            out.append(h)
        elif h.startswith("/"):
            root = "/".join(base_url.split("/")[:3])
            out.append(root + h)
        else:
            out.append(base_url + h.split("/")[-1])
    return out


def _parse_zip(content: bytes) -> pd.DataFrame | None:
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                return None
            raw = zf.read(csv_names[0]).decode("utf-8", errors="ignore")

        header, rows = None, []
        for line in raw.splitlines():
            parts = line.split(",")
            if not parts:
                continue
            tag = parts[0].strip().upper().lstrip("﻿")
            if (tag == "I" and len(parts) > 4
                    and parts[1].upper() == "DISPATCH"
                    and parts[2].upper() in ("UNIT_SCADA", "UNITSCADA")):
                header = [c.strip().upper() for c in parts[4:]]
            elif (tag == "D" and len(parts) > 4
                    and parts[1].upper() == "DISPATCH"
                    and parts[2].upper() in ("UNIT_SCADA", "UNITSCADA")):
                rows.append(parts[4:])

        if not header or not rows:
            return None

        df = pd.DataFrame(rows, columns=header[:len(rows[0])])
        req = {"SETTLEMENTDATE", "DUID", "SCADAVALUE"}
        if not req.issubset(df.columns):
            return None

        df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], errors="coerce")
        df["mw"] = pd.to_numeric(df["SCADAVALUE"], errors="coerce")
        df = df.dropna(subset=["SETTLEMENTDATE", "mw"])
        df["ts_end_nem"] = df["SETTLEMENTDATE"].dt.tz_localize(TZ_NEM)
        df["ts_start_nem"] = df["ts_end_nem"] - pd.Timedelta(minutes=5)

        return df[["DUID", "ts_start_nem", "ts_end_nem", "mw"]].copy()
    except Exception:
        return None


def _fetch_one(url: str) -> pd.DataFrame | None:
    try:
        r = requests.get(url, timeout=30, headers=_HEADERS)
        r.raise_for_status()
        return _parse_zip(r.content)
    except Exception:
        return None


def fetch_date_range(start: date, end: date, workers: int = 20) -> dict[str, int]:
    """
    Download all Dispatch_SCADA interval files covering start..end.
    Returns {YYYY-MM: row_count} for months that were written.
    """
    date_strs = set()
    d = start
    while d <= end:
        date_strs.add(d.strftime("%Y%m%d"))
        d += timedelta(days=1)

    all_urls: list[str] = []
    for base in (ARCHIVE_URL, CURRENT_URL):
        try:
            all_urls.extend(_list_zip_urls(base))
        except Exception:
            pass

    wanted = list({
        u for u in all_urls
        if "SCADA" in u.upper() and any(ds in u for ds in date_strs)
    })

    if not wanted:
        return {}

    frames: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for df in pool.map(_fetch_one, wanted):
            if df is not None and not df.empty:
                frames.append(df)

    if not frames:
        return {}

    combined = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["DUID", "ts_end_nem"], keep="last")
    )
    combined = combined[
        (combined["ts_end_nem"].dt.date >= start) &
        (combined["ts_end_nem"].dt.date <= end)
    ]
    if combined.empty:
        return {}

    results: dict[str, int] = {}
    for (y, m), grp in combined.groupby(
        [combined["ts_end_nem"].dt.year, combined["ts_end_nem"].dt.month]
    ):
        out = CURATED / f"scada_{y}-{m:02}.parquet"
        if out.exists():
            existing = pd.read_parquet(out)
            grp = (
                pd.concat([existing, grp], ignore_index=True)
                .drop_duplicates(subset=["DUID", "ts_end_nem"], keep="last")
                .sort_values(["DUID", "ts_end_nem"])
            )
        else:
            grp = grp.sort_values(["DUID", "ts_end_nem"])
        grp.to_parquet(out, index=False)
        key = f"{y}-{m:02}"
        results[key] = len(grp)

    return results
