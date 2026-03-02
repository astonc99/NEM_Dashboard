#imports
from pathlib import Path
import requests
import pandas as pd
from dateutil import tz
import re, zipfile, io
from urllib.parse import urljoin
import csv

#Directiories 
RAW_MMS = Path(__file__).resolve().parents[1] / "data" / "raw" / "mmsdm" / "dispatch_unit_scada"
RAW_MMS.mkdir(parents=True, exist_ok=True)

CURATED = Path(__file__).resolve().parents[1] / "data" / "curated" / "scada"
CURATED.mkdir(parents=True, exist_ok=True)

#NEM time (AEST, no daylight saving) – AEMO stamps settlement at end-of-interval
TZ_NEM = tz.gettz("Etc/GMT-10") 

#Find URLS
def find_dispatch_unit_scada_archive_url(base_dir_url: str) -> str:
    """
    Discover the monthly archive zip for DISPATCH_UNIT_SCADA within the MMSDM DATA directory.
    Excludes PREDISPATCH*.
    """
    r = requests.get(base_dir_url, timeout=60)
    r.raise_for_status()
    hrefs = re.findall(r'href="([^"]+\.zip)"', r.text, flags=re.IGNORECASE)
    if not hrefs:
        raise RuntimeError("No .zip files found for SCADA")
    
    def is_scada(h: str) -> bool:
        u = h.upper()
        return ("DISPATCH_UNIT_SCADA" in u or "DISPATCHUNITSCADA" in u) and ("PREDISPATCH" not in u)
    
    candidates = [h for h in hrefs if is_scada(h)]
    if not candidates:
        raise RuntimeError("No DISPATCH_UNIT_SCADA .zip found in this MMSDM DATA folder")
    
    return urljoin(base_dir_url, candidates[0])


#Download URL and find and save CSV for output directoty
def download_and_extract_csv(archive_url: str, out_dir: Path) -> Path:
    #Make saving directories if they do not exist
    out_dir.mkdir(parents=True, exist_ok=True)
    #Connect to website and initiate connection
    resp = requests.get(archive_url, timeout=120)
    resp.raise_for_status()
    #Find last  bit of zipfile name
    zip_name = archive_url.split("/")[-1]
    # Save raw zip to saving directory
    raw_zip_path = out_dir / zip_name
    #If no zip dirctory then create
    if not raw_zip_path.exists():
        raw_zip_path.write_bytes(resp.content)
    #Open zipe file and store csv files
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV found inside archive: {zip_name}")
        #Take first zip file name
        csv_names.sort(key=lambda n: (("scada" not in n.lower(), n.lower())))
        csv_names = csv_names[0]
        csv_path = out_dir / csv_names
        if not csv_path.exists():
            csv_path.write_bytes(zf.read(csv_names))
        return csv_path
    
# True or false if meets C/I/D format 
def is_sqlloader_format(csv_path: Path) -> bool:
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        for _ in range(10):
            line = f.readline()
            if not line:
                break
            #Strips invisible lines that may cause file processing errors
            s = line.lstrip("\ufeff").strip()  # strip BOM if present
            if not s:
                continue
            #Next line checks and says true or false if starting with ...
            return s.startswith(("C,", "I,", "D"))
        return False

#Extracting relevant data from csv using pandas
def load_scada_sqlloader(csv_path: Path) -> pd.DataFrame:
    header_fields = None
    data_rows = []
    
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            tag = row[0].lstrip("\ufeff").strip().upper()  # handle BOM + normalize
            # Extract header lines
            if tag == "I" and len(row) >= 4 and row[1].upper() == "DISPATCH" and row[2].upper() in ("UNIT_SCADA", "UNITSCADA"):
                header_fields = [c.strip().upper() for c in row[4:]]
                continue
            #Data lines taht start with D
            if tag == "D" and len(row) >= 4 and row[1].upper() == "DISPATCH" and row[2].upper() in ("UNIT_SCADA", "UNITSCADA"):
                data_rows.append(row[4:])
        
    if header_fields is None:
        raise RuntimeError("Missing I,DISPATCH,UNIT_SCADA,5 header in SQL-Loader file.")
    if not data_rows:
        return pd.DataFrame(columns=header_fields)
    
    df = pd.DataFrame(data_rows, columns=header_fields)
    return df
    
#Fallback for plain header CSV
def load_scada_header_csv(csv_path: Path) -> pd.DataFrame:
    usecols = ["SETTLEMENTDATE", "DUID", "SCADAVALUE"]
    df = pd.read_csv(csv_path, usecols=usecols, low_memory=False)
    df.columns = [c.strip().upper() for c in df.columns]
    return df

#Normalise into tidy SCADA with NEM timestamps
def normalise_scada(csv_path: Path) -> pd.DataFrame:
    if is_sqlloader_format(csv_path):
        df = load_scada_sqlloader(csv_path)
    else:
        df = load_scada_header_csv(csv_path)
    
    df.columns = [str(c).strip().upper() for c in df.columns]

    for req in ("SETTLEMENTDATE", "DUID", "SCADAVALUE"):
        if req not in df.columns:
            raise KeyError(f"Required column missing: {req}. Columns: {', '.join(df.columns)}")
    
    #Restandardise the data
    df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], errors="coerce")
    df = df.dropna(subset=["SETTLEMENTDATE"])
    df["ts_end_nem"] = df["SETTLEMENTDATE"].dt.tz_localize(TZ_NEM)
    df["ts_start_nem"] = df["ts_end_nem"] - pd.Timedelta(minutes=5)

    df["mw"] = pd.to_numeric(df["SCADAVALUE"], errors="coerce")
    df = df.dropna(subset=["mw"])

    out = df[["DUID", "ts_start_nem", "ts_end_nem", "mw"]].copy()
    #Sort and De=dup by DUID, ts_end_nem keeping last
    out = (out.sort_values(["DUID","ts_end_nem"])).drop_duplicates(subset=["DUID", "ts_end_nem"], keep="last").reset_index(drop=True)

    return out
# Write to Parquet file

def write_parquet_month(df: pd.DataFrame) -> Path:
    if df.empty:
        raise ValueError("No Rows to write to")
    
    y = int(df["ts_end_nem"].dt.year.iloc[0])
    m = int(df["ts_end_nem"].dt.month.iloc[0])
    out = CURATED / f"scada_{y}-{m:02}.parquet"

    if out.exists():
        existing = pd.read_parquet(out)
        combined = (pd.concat([existing, df], ignore_index=True).drop_duplicates(subset=["DUID","ts_end_nem"], keep="last").sort_values(["DUID","ts_end_nem"]))
        combined.to_parquet(out, index=False)
    else:
        df.to_parquet(out, index=False)
    
    return out

#Write whole all together

def backfill_one(year: int, month: int) -> Path:
    """
    Backfill one month of DISPATCH_UNIT_SCADA from MMSDM Monthly Archive.
    Returns the path to the written Parquet.
    """
    base_dir = (
        f"https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
        f"{year}/MMSDM_{year}_{month:02}/MMSDM_Historical_Data_SQLLoader/DATA/"
    )

    # 1) Discover archive
    archive_url = find_dispatch_unit_scada_archive_url(base_dir)

    # 2) Download & extract
    month_raw_dir = RAW_MMS / f"{year}-{month:02d}"
    csv_path = download_and_extract_csv(archive_url, month_raw_dir)

    # 3) Normalise & constrain to target month (belt-and-braces)
    df = normalise_scada(csv_path)
    yyyymm = f"{year:04d}-{month:02d}"
    df = df[df["ts_end_nem"].dt.strftime("%Y-%m") == yyyymm]

    # 4) Write parquet
    return write_parquet_month(df)


if __name__ == "__main__":
    # quick smoke test
    print(backfill_one(2024, 7))
