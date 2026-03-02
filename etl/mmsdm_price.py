from pathlib import Path
import requests
import pandas as pd
from dateutil import tz
import re, zipfile, io
from urllib.parse import urljoin
import csv


# Directories
RAW_MMS = Path(__file__).resolve().parents[1] / "data" / "raw" / "mmsdm" / "dispatchprice"
RAW_MMS.mkdir(parents=True, exist_ok=True)

CURATED = Path(__file__).resolve().parents[1] / "data" / "curated" / "prices"
CURATED.mkdir(parents=True, exist_ok=True)

# NEM time (AEST, no daylight saving) – AEMO stamps settlement at end-of-interval
TZ_NEM = tz.gettz("Etc/GMT-10")


def find_dispatchprice_archive_url(base_dir_url: str) -> str:
    """
    Discover the correct DISPATCHPRICE monthly archive in an MMSDM DATA directory.
    Excludes PREDISPATCH* tables to ensure we get actual (not forecast) prices.
    """
    r = requests.get(base_dir_url, timeout=60)
    r.raise_for_status()

    # Find any .zip link that includes DISPATCHPRICE but not PREDISPATCH
    # Works for names like:
    #   PUBLIC_ARCHIVE#DISPATCHPRICE#FILE01#202509010000.zip
    #   PUBLIC_DVD_DISPATCHPRICE_202509010000.zip  (if present)
    hrefs = re.findall(r'href="([^"]+\.zip)"', r.text, flags=re.IGNORECASE)

    # Filter to dispatch price (actuals) only
    candidates = [
        h for h in hrefs
        if ("DISPATCHPRICE" in h.upper()) and ("PREDISPATCH" not in h.upper())
    ]
    if not candidates:
        raise RuntimeError("No DISPATCHPRICE .zip found in this MMSDM DATA folder")

    # If multiple files exist, pick the first (or sort to pick latest by name)
    return urljoin(base_dir_url, candidates[0])


def download_and_extract_csv(archive_url: str, out_dir: Path) -> Path:
    """
    Download the archive zip and extract the contained CSV to out_dir.
    Returns the path to the extracted CSV.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Download
    resp = requests.get(archive_url, timeout=120)
    resp.raise_for_status()

    # Save raw zip (optional caching)
    zip_name = archive_url.split("/")[-1]
    raw_zip_path = out_dir / zip_name
    if not raw_zip_path.exists():
        raw_zip_path.write_bytes(resp.content)

    # Extract the contained CSV (assume a single CSV inside)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV found inside archive: {zip_name}")
        csv_name = csv_names[0]
        csv_path = out_dir / csv_name
        if not csv_path.exists():
            csv_path.write_bytes(zf.read(csv_name))
        return csv_path


def is_sqlloader_format(csv_path: Path) -> bool:
    """True if first non-empty lines start with C,/I,/D, indicating SQL-Loader format."""
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        for _ in range(10):
            line = f.readline()
            if not line:
                break
            s = line.lstrip("\ufeff").strip()  # strip BOM if present
            if not s:
                continue
            return s.startswith(("C,", "I,", "D,"))
    return False

def load_dispatchprice_sqlloader(csv_path: Path) -> pd.DataFrame:
    """
    Parse an MMSDM Monthly 'SQL-Loader' style DISPATCHPRICE file.
    - Use I-line after first 4 tokens as header.
    - Use D-lines after first 4 tokens as data.
    """
    header_fields = None
    data_rows = []

    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            tag = row[0].lstrip("\ufeff").strip().upper()  # handle BOM + normalize
            # I line: header
            if tag == "I" and len(row) >= 5 and row[1].upper() == "DISPATCH" and row[2].upper() == "PRICE":
                header_fields = [c.strip().upper() for c in row[4:]]
            # D line: data
            elif tag == "D" and len(row) >= 5 and row[1].upper() == "DISPATCH" and row[2].upper() == "PRICE":
                data_rows.append(row[4:])

    if header_fields is None:
        raise RuntimeError("Missing I,DISPATCH,PRICE,5 header in SQL-Loader file.")
    if not data_rows:
        raise RuntimeError("No D,DISPATCH,PRICE,5 data rows in SQL-Loader file.")
    print(len(header_fields), "header fields found in SQL-Loader file.")
    print(len(data_rows), "data rows found in SQL-Loader file.")
    
    df = pd.DataFrame(data_rows, columns=header_fields)
    return df


def normalise_vic(csv_path: Path) -> pd.DataFrame:
    """
    Load DISPATCHPRICE for one month (either SQL-Loader C/I/D or normal CSV),
    filter to VIC1, build tidy timestamps.
    """
    if is_sqlloader_format(csv_path):
        df = load_dispatchprice_sqlloader(csv_path)
    else:
        # Fallback: plain headered CSV (rare for MMSDM dispatchprice; still safe)
        df = pd.read_csv(csv_path, low_memory=False)

    # Canonicalise column names
    df.columns = [str(c).strip().upper() for c in df.columns]

    # Pick timestamp column
    ts_col = "SETTLEMENTDATE" if "SETTLEMENTDATE" in df.columns else (
        "INTERVAL_DATETIME" if "INTERVAL_DATETIME" in df.columns else None
    )
    if ts_col is None:
        raise KeyError("No SETTLEMENTDATE or INTERVAL_DATETIME in columns: " + ", ".join(df.columns))

    # Required fields
    for req in ("REGIONID", "RRP"):
        if req not in df.columns:
            raise KeyError(f"Required column missing: {req}. Columns: {', '.join(df.columns)}")

    # Filter VIC1
    df = df[df["REGIONID"] == "VIC1"].copy()
    if df.empty:
        # Return consistent schema even if this month has no VIC1 rows (it will)
        return pd.DataFrame(columns=["ts_start_nem","ts_end_nem","region","rrp"])

    # Cast types
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    df["RRP"] = pd.to_numeric(df["RRP"], errors="coerce")

    # AEMO uses end-of-interval stamping; keep NEM time
    df["ts_end_nem"] = df[ts_col].dt.tz_localize(TZ_NEM)
    df["ts_start_nem"] = df["ts_end_nem"] - pd.Timedelta(minutes=5)

    out = df[["ts_start_nem","ts_end_nem","REGIONID","RRP"]].rename(
        columns={"REGIONID":"region","RRP":"rrp"}
    )
    return out.sort_values("ts_end_nem").reset_index(drop=True)


def write_parquet_month(df: pd.DataFrame) -> Path:
    """Write/ append one month of prices to curated Parquet with de-duplication"""

    if df.empty:
        raise ValueError("No Rows to write to")
    
    y = int(df["ts_end_nem"].dt.year.iloc[0])
    m = int(df["ts_end_nem"].dt.month.iloc[0])
    out = CURATED / f"prices_{y}-{m:02}.parquet"
    
    if out.exists():
        existing = pd.read_parquet(out)
        combined = (pd.concat([existing, df], ignore_index=True).drop_duplicates(subset=["ts_end_nem"], keep="last").sort_values("ts_end_nem"))
        combined.to_parquet(out, index=False)
    else:
        df.to_parquet(out, index=False)
    return out

def backfill_one(year: int, month: int) -> Path:
    """
    Backfill one month of 5-minute prices (DISPATCHPRICE) from MMSDM Monthly Archive.
    """
    base_dir = (
        f"https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
        f"{year}/MMSDM_{year}_{month:02}/MMSDM_Historical_Data_SQLLoader/DATA/"
    )

    # 1) Discover the correct DISPATCHPRICE archive for that month
    archive_url = find_dispatchprice_archive_url(base_dir)

    # 2) Download and extract the CSV into our raw cache
    csv_path = download_and_extract_csv(archive_url, RAW_MMS)

    # 3) Normalise and write to curated Parquet (reuse your existing functions)
    df = normalise_vic(csv_path)
    return write_parquet_month(df)


if __name__ == "__main__":
    print(backfill_one(2025,9))

