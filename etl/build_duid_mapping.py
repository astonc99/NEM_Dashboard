#etl/build_duid+mapping.py - this script finds prexisting duid data from aemo's registration register and maps it to our internal duid database

from pathlib import Path
import pandas as pd
import requests
import io, re
from urllib.parse import urljoin

RAW_DATA_DIR = Path("data/raw/aemo_ref"); RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = Path("data/static/duid_fuel.csv"); OUT_CSV.parent.mkdir(parents=True, exist_ok=True)


AEMO_REG_PAGE = "https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/participate-in-the-market/registration"
REG_XLS_FALLBACK = "https://aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls"

def find_registation_workbook_url() -> str:
    # tries absolute link first then has a look for the otherlink if it cannot connect
    try:
        r = requests.get(REG_XLS_FALLBACK, timeout=15, allow_redirects=True)
        if 200 <= r.status_code() < 400:
            return REG_XLS_FALLBACK
    except:
        pass

    try:
        r = requests.get(AEMO_REG_PAGE, timeout=60)
        r.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        # some clients (including our workspace) get a 403 on the HTML page;
        # just use the known fallback spreadsheet instead of scraping the page.
        print(f"Warning: failed to fetch registration page ({exc}), using fallback")
        return REG_XLS_FALLBACK

    matches = re.findall(r'href="([^"]?+\.(?:xlsx?|XLSX?))"', r.text, flags=re.IGNORECASE)
    if not matches:
        raise ValueError("Could not find registration workbook link on AEMO registration page")
    preferred = [m for m in matches if re.search(r'(Registration|Exemption|NEM)', m, flags=re.IGNORECASE)]
    url = (preferred[0] if preferred else matches[0]).replace("&amp;", "&")
    return urljoin(AEMO_REG_PAGE, url)


# now to download and cache the sheet

def download_workbook(url: str) -> bytes:
    # Some AEMO URLs reject non-browser clients; add a simple user-agent and
    # allow redirects to avoid 403 errors.
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Python)"}
    try:
        r = requests.get(url, timeout=60, allow_redirects=True, headers=headers)
        r.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(f"Unable to fetch workbook ({exc})") from exc

    if not r.ok:
        raise ValueError(f"Failed to download workbook from {url}")
    name = url.split("/")[-1].split("?")[0]
    cache_path = RAW_DATA_DIR / name
    if not cache_path.exists():
        cache_path.write_bytes(r.content)
    return r.content


# now to read the workbook and extract the duid and fuel type data

def read_generators_sheet(xl_bytes: bytes) -> pd.DataFrame:
    """
    Prefer the 'PU and Scheduled Loads' sheet (exact name, case-insensitive).
    If not found, try a loose contains match.
    Finally, fall back to any sheet that contains a DUID column.
    Keep only rows with a non-empty DUID.
    """
    xls = pd.ExcelFile(io.BytesIO(xl_bytes))  # pandas will pick engine; openpyxl works for .xlsx

    # 1) Exact (case-insensitive) match
    target = next((s for s in xls.sheet_names if s.strip().lower() == "pu and scheduled loads"), None)

    # 2) Loose contains, if exact not found (handles minor naming tweaks)
    if target is None:
        target = next(
            (s for s in xls.sheet_names if "pu" in s.lower() and "scheduled" in s.lower() and "load" in s.lower()),
            None
        )

    # 3) Fallback: any sheet with a DUID column
    if target is None:
        for s in xls.sheet_names:
            try:
                head = pd.read_excel(xls, sheet_name=s, nrows=5, dtype=str)
            except Exception:
                continue
            head.columns = [c.strip() for c in head.columns]
            if any(c.strip().upper() == "DUID" for c in head.columns):
                target = s
                break

    if target is None:
        raise RuntimeError("Could not find a sheet named 'PU and Scheduled Loads' or any sheet with a DUID column.")

    # Read the chosen sheet
    df = pd.read_excel(xls, sheet_name=target, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    
    required_any_of = [    ("Station Name", "Station", "Power Station Name", "Power Station"),    ("Region", "RegionID", "Region Id", "Region ID"),    ("Fuel Source - Primary", "Fuel Source – Primary", "Fuel"),    ("Technology Type - Primary", "Technology Type – Primary", "Technology"),]
    for alts in required_any_of:
        if not any(col.lower() in [a.lower() for a in alts] for col in df.columns):
        # Not fatal (we can still classify with partial info), but helpful to know        
            print(f"Warning: none of {alts} present on '{target}'. Proceeding with available columns.")

    # Keep only rows with a DUID
    if "DUID" not in df.columns:
        raise RuntimeError(f"'DUID' column not present in sheet: {target}")

    df = df[df["DUID"].astype(str).str.strip() != ""].reset_index(drop=True)

    # Helpful debug print
    print(f"Using sheet: {target} (rows with DUID: {len(df)})")

    return df

def _col(df: pd.DataFrame, *alts: str)-> str| None:
    alts_lc = [a.lower() for a in alts]
    for c in df.columns:
        if c.lower() in alts_lc:
            return c
    return None

def _clean(x) -> str:
    return "" if x is None else str(x).strip()

def classify_fuel(primary: str, tech: str) -> str:
    p, t= _clean(primary).upper(), _clean(tech).upper()
    if "BATTERY" in p or "BATTERY" in t: return "Battery"
    if "SOLAR" in p or "SOLAR" in t or "PV" in t: return "Solar - Utility"
   
    if "WIND" in p or "WIND" in t: return "Wind"
    if "HYDRO" in p or "HYDRO" in t or p == "WATER": return "Hydro"
    if "COAL" in p: return "Brown Coal" if "BROWN" in p else "Black Coal"
    if "GAS" in p or "METHANE" in p:
        if any(k in t for k in ("COMBINED CYCLE","CCGT")): return "Gas – CCGT"
        if any(k in t for k in ("OPEN CYCLE","OCGT","ENGINE","RECIPROCATING","PEAKER")): return "Gas – OCGT/Peaker"
        return "Gas – Other"
    if any(k in p for k in ("BAGASSE","BIOMASS","LANDFILL","SEWAGE","WASTE GAS")): return "Bioenergy"
    if any(k in p for k in ("DIESEL","DISTILLATE","KEROSENE")): return "Liquid Fuel"
    return "Other"

def build_mapping(df_raw: pd.DataFrame) -> pd.DataFrame:
    duid_col = "DUID"
    station_col = _col(df_raw, "Station Name","Station","Power Station Name","Power Station")
    region_col  = _col(df_raw, "Region","RegionID","Region Id","Region ID")
    fuel_col    = _col(df_raw, "Fuel Source - Primary","Fuel Source – Primary","Fuel")
    tech_col    = _col(df_raw, "Technology Type - Primary","Technology Type – Primary","Technology")

    
    fuels = [
        classify_fuel(df_raw.at[i, fuel_col] if fuel_col else "", df_raw.at[i, tech_col] if tech_col else "")
        for i in df_raw.index
    ]

    out = pd.DataFrame({
        "DUID":    df_raw[duid_col].astype(str).str.upper().str.strip(),
        "Station": df_raw[station_col].astype(str).str.strip() if station_col else "",
        "Fuel":    pd.Series(fuels, index=df_raw.index),
        "Region":  df_raw[region_col].astype(str).str.upper().str.strip() if region_col else "",
    })
    return (out[out["DUID"] != ""]
            .drop_duplicates(subset=["DUID"], keep="last")
            .sort_values("DUID")
            .reset_index(drop=True))



def main():
    url = find_registation_workbook_url()
    print("Registration workbook:", url)
    content = download_workbook(url)
    raw = read_generators_sheet(content)
    mapping = build_mapping(raw)
    OUT_CSV.write_text(mapping.to_csv(index=False))
    print(f"Wrote mapping to: {OUT_CSV}")
    print("\nDUID count by Fuel:")
    print(mapping["Fuel"].value_counts().to_string())

if __name__ == "__main__":
    main()