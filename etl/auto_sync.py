"""
Smart sync orchestrator.
- Uses MMSDM for complete months published >6 weeks ago.
- Uses NEMWeb Archive for recent months not yet in MMSDM.
- Caps at max_months to avoid very long syncs (call again to continue).
- start_date / end_date overrides let callers specify a fixed range.
"""
import calendar
from datetime import date, timedelta
from pathlib import Path
import pandas as pd


def _latest_date(parquet_glob: str) -> date | None:
    files = sorted(Path(".").glob(parquet_glob))
    if not files:
        return None
    latest = None
    for p in files[-4:]:
        try:
            df = pd.read_parquet(p, columns=["ts_end_nem"])
            m = df["ts_end_nem"].max()
            if latest is None or m > latest:
                latest = m
        except Exception:
            pass
    return latest.date() if latest is not None else None


def _mmsdm_published(year: int, month: int) -> bool:
    """MMSDM typically publishes ~6 weeks after month end."""
    last_day = date(year, month, calendar.monthrange(year, month)[1])
    return (date.today() - last_day).days > 42


def _next_month(y: int, m: int) -> tuple[int, int]:
    return (y + 1, 1) if m == 12 else (y, m + 1)


def sync_prices(
    start_date: date | None = None,
    end_date: date | None = None,
    max_months: int = 12,
) -> dict:
    from etl import mmsdm_price, nemweb_price

    today = end_date or date.today()

    if start_date:
        # Explicit range — start from the first day of the given month
        start = date(start_date.year, start_date.month, 1)
    else:
        # Auto-detect from latest stored data
        latest = _latest_date("data/curated/prices/prices_*.parquet")
        start = (latest + timedelta(days=1)) if latest else date(today.year, today.month, 1)

    if start > today:
        return {"status": "up_to_date", "months_synced": [], "errors": [],
                "latest_date": str(start - timedelta(days=1))}

    months_synced, errors = [], []
    y, m = start.year, start.month
    processed = 0

    while (y, m) <= (today.year, today.month) and processed < max_months:
        month_start = date(y, m, 1)
        month_end   = date(y, m, calendar.monthrange(y, m)[1])

        if _mmsdm_published(y, m):
            try:
                mmsdm_price.backfill_one(y, m)
                months_synced.append(f"{y}-{m:02}")
            except Exception as e:
                errors.append({"month": f"{y}-{m:02}", "source": "mmsdm", "error": str(e)})
        else:
            fetch_start = max(start, month_start)
            fetch_end   = min(today, month_end)
            try:
                result = nemweb_price.fetch_date_range(fetch_start, fetch_end)
                months_synced.extend(result.keys())
                if not result:
                    errors.append({"month": f"{y}-{m:02}", "source": "nemweb",
                                   "error": "No files found in Archive for this date range"})
            except Exception as e:
                errors.append({"month": f"{y}-{m:02}", "source": "nemweb", "error": str(e)})

        y, m = _next_month(y, m)
        processed += 1

    new_latest = _latest_date("data/curated/prices/prices_*.parquet")
    return {
        "status": "ok",
        "months_synced": months_synced,
        "errors": errors,
        "latest_date": str(new_latest) if new_latest else None,
        "more_available": (y, m) <= (today.year, today.month),
    }


def sync_scada(
    start_date: date | None = None,
    end_date: date | None = None,
    max_months: int = 12,
) -> dict:
    from etl import mmsdm_scada, nemweb_scada

    today = end_date or date.today()

    if start_date:
        start = date(start_date.year, start_date.month, 1)
    else:
        latest = _latest_date("data/curated/scada/scada_*.parquet")
        start = (latest + timedelta(days=1)) if latest else date(today.year, today.month, 1)

    if start > today:
        return {"status": "up_to_date", "months_synced": [], "errors": [],
                "latest_date": str(start - timedelta(days=1))}

    months_synced, errors = [], []
    y, m = start.year, start.month
    processed = 0

    while (y, m) <= (today.year, today.month) and processed < max_months:
        month_start = date(y, m, 1)
        month_end   = date(y, m, calendar.monthrange(y, m)[1])

        if _mmsdm_published(y, m):
            try:
                mmsdm_scada.backfill_one(y, m)
                months_synced.append(f"{y}-{m:02}")
            except Exception as e:
                errors.append({"month": f"{y}-{m:02}", "source": "mmsdm", "error": str(e)})
        else:
            fetch_start = max(start, month_start)
            fetch_end   = min(today, month_end)
            try:
                result = nemweb_scada.fetch_date_range(fetch_start, fetch_end)
                months_synced.extend(result.keys())
                if not result:
                    errors.append({"month": f"{y}-{m:02}", "source": "nemweb",
                                   "error": "No files found in Archive for this date range"})
            except Exception as e:
                errors.append({"month": f"{y}-{m:02}", "source": "nemweb", "error": str(e)})

        y, m = _next_month(y, m)
        processed += 1

    new_latest = _latest_date("data/curated/scada/scada_*.parquet")
    return {
        "status": "ok",
        "months_synced": months_synced,
        "errors": errors,
        "latest_date": str(new_latest) if new_latest else None,
        "more_available": (y, m) <= (today.year, today.month),
    }
