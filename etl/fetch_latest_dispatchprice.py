# PLEASE NOTE THIS IS BASED ON GETTING 5MIN DATA AND DOES NOT READ THE CID sql lOADER FORMATTINGN CORRECTLY SO SHOUD BE IGNORED


import io, zipfile, re, os
from pathlib import Path
import requests
import pandas as pd
from dateutil import tz

CURATED_PRICES_DIR = Path(__file__).resolve().parents[1] / "data" / "curated" / "prices"
CURATED_PRICES_DIR.mkdir(parents=True, exist_ok=True)
BASE = "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
TZ_NEM = tz.gettz("Etc/GMT-10") # NEM time (AEST, no DST)

def latest_zip_name():
	r = requests.get(BASE, timeout = 30)
	r.raise_for_status()
	# Grab hrefs that end in .zip (simple, but it works)
	hrefs = re.findall(r'href="([^"]+\.zip)"', r.text, flags=re.IGNORECASE)
	if not hrefs:
		raise RuntimeError("No .zip files found on the index.")
	
	names = [os.path.basename(h) for h in hrefs]

	return names[-1] # latest is last

def fetch_latest_df():
	name = latest_zip_name()
	url = BASE + name
	print(f"Downloading: {url}")
	resp = requests.get(url, timeout=60)
	resp.raise_for_status()

	with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
		csv_name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
		with zf.open(csv_name) as f:
			df = pd.read_csv(f)
	return df

def to_vic_tidy(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=str.upper)
    df = df[df["REGIONID"] == "VIC1"].copy()
    # SETTLEMENTDATE is end-of-interval; make timezone-aware
    df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], errors="coerce")
    df["ts_end_nem"] = df["SETTLEMENTDATE"].dt.tz_localize(TZ_NEM)
    df["ts_start_nem"] = df["ts_end_nem"] - pd.Timedelta(minutes=5)
    # Keep only the columns we need at first
    out = df[["ts_start_nem", "ts_end_nem", "REGIONID", "RRP"]].rename(
        columns={"REGIONID": "region", "RRP": "rrp"}
    )
    return out.sort_values("ts_end_nem").reset_index(drop=True)

def write_month_parquet(df_vic: pd.DataFrame) -> list[Path]:
	"""Write/append to one Parquet per month; return list of written paths"""
	if df_vic.empty:
		return []
	#month groups by tz-aware ts_end_nem
	df_vic = df_vic.copy()
	df_vic["year"] = df_vic["ts_end_nem"].dt.year
	df_vic["month"] = df_vic["ts_end_nem"].dt.month

	written = []
	for (y,m), g in df_vic.groupby(["year","month"], dropna=False):
		out = CURATED_PRICES_DIR /f"prices_{y}-{m:02}.parquet"
		g2 = g.drop(columns=["year", "month"])
		if out.exists():
			existing = pd.read_parquet(out)
			combined = pd.concat([existing,g2], ignore_index=True).drop_duplicates(subset=["ts_end_nem"], keep="last").sort_value("ts_end_nem")
			combined.to_parquet(out, Index=False)
		else:
			g2.sort_values("ts_end_nem").to_parquet(out, Index=False)
		written.append(out)
	return written	

def main():
	raw = fetch_latest_df()
	vic = to_vic_tidy(raw)
	print(vic.tail(10).to_sring(index=False))
	paths = write_month_parquet(vic)
	print("Wrote:", [p.name for p in paths])

if __name__ == "__main__":
	main()



