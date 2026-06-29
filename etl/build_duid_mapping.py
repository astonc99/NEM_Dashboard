# etl/build_duid_mapping.py
# Downloads AEMO's Registration & Exemption List and builds duid_fuel.csv.
# Run from the project root: python etl/build_duid_mapping.py

from pathlib import Path
import pandas as pd
import requests
import io, re, unicodedata

RAW_DATA_DIR = Path("data/raw/aemo_ref")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = Path("data/static/duid_fuel.csv")
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

AEMO_REG_PAGE   = "https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/participate-in-the-market/registration"
REG_XLS_FALLBACK = "https://aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls"

# ── Manual overrides applied after automated classification ───────────────────
# Add DUIDs here when the AEMO workbook classifies them incorrectly.
# Format: "DUID": "Fuel label"   (use plain ASCII hyphens)
MANUAL_OVERRIDES: dict[str, str] = {
    # VIC1 – Brown Coal
    "LOYYB1": "Brown Coal", "LOYYB2": "Brown Coal",
    "LYA1":   "Brown Coal", "LYA2":   "Brown Coal",
    "LYA3":   "Brown Coal", "LYA4":   "Brown Coal",
    "YWPS1":  "Brown Coal", "YWPS2":  "Brown Coal",
    "YWPS3":  "Brown Coal", "YWPS4":  "Brown Coal",
    # VIC1 – Gas peakers
    "AGLSOM":   "Gas - OCGT/Peaker",
    "BDL01":    "Gas - OCGT/Peaker", "BDL02":    "Gas - OCGT/Peaker",
    "HASTING1": "Gas - OCGT/Peaker", "HASTING2": "Gas - OCGT/Peaker",
    "HASTING3": "Gas - OCGT/Peaker",
    "JLA01":    "Gas - OCGT/Peaker", "JLA02":    "Gas - OCGT/Peaker",
    "JLA03":    "Gas - OCGT/Peaker", "JLA04":    "Gas - OCGT/Peaker",
    "JLB01":    "Gas - OCGT/Peaker", "JLB02":    "Gas - OCGT/Peaker",
    "JLB03":    "Gas - OCGT/Peaker",
    "LNGS1":    "Gas - OCGT/Peaker", "LNGS2":    "Gas - OCGT/Peaker",
    "MORTLK11": "Gas - OCGT/Peaker", "MORTLK12": "Gas - OCGT/Peaker",
    "VPGS1":    "Gas - OCGT/Peaker", "VPGS2":    "Gas - OCGT/Peaker",
    "VPGS3":    "Gas - OCGT/Peaker", "VPGS4":    "Gas - OCGT/Peaker",
    "VPGS5":    "Gas - OCGT/Peaker", "VPGS6":    "Gas - OCGT/Peaker",
    # VIC1 – Gas other
    "NPS":    "Gas - Other",
    "TGNSS1": "Gas - Other",
    # VIC1 – Interconnector
    "BLNKVIC": "Interconnector",
    "BLNKTAS": "Interconnector",
}


def find_registration_workbook_url() -> str:
    try:
        r = requests.get(AEMO_REG_PAGE, timeout=60)
        r.raise_for_status()
        matches = re.findall(r'href="([^"]+\.(?:xlsx?|XLSX?))"', r.text, re.IGNORECASE)
        if matches:
            preferred = [m for m in matches if re.search(r"(Registration|Exemption|NEM)", m, re.I)]
            from urllib.parse import urljoin
            url = (preferred[0] if preferred else matches[0]).replace("&amp;", "&")
            return urljoin(AEMO_REG_PAGE, url)
    except Exception as exc:
        print(f"Warning: failed to fetch registration page ({exc}), using fallback")
    return REG_XLS_FALLBACK


def download_workbook(url: str) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Python)"}
    r = requests.get(url, timeout=60, allow_redirects=True, headers=headers)
    r.raise_for_status()
    name = url.split("/")[-1].split("?")[0]
    cache_path = RAW_DATA_DIR / name
    cache_path.write_bytes(r.content)
    return r.content


def read_generators_sheet(xl_bytes: bytes) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(xl_bytes))
    target = next((s for s in xls.sheet_names if s.strip().lower() == "pu and scheduled loads"), None)
    if target is None:
        target = next((s for s in xls.sheet_names
                       if "pu" in s.lower() and "scheduled" in s.lower()), None)
    if target is None:
        for s in xls.sheet_names:
            try:
                head = pd.read_excel(xls, sheet_name=s, nrows=5, dtype=str)
                if any(c.strip().upper() == "DUID" for c in head.columns):
                    target = s
                    break
            except Exception:
                continue
    if target is None:
        raise RuntimeError("Could not find 'PU and Scheduled Loads' sheet.")

    df = pd.read_excel(xls, sheet_name=target, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df["DUID"].astype(str).str.strip() != ""].reset_index(drop=True)
    print(f"Sheet: {target!r}  ({len(df)} rows with DUID)")
    print(f"Columns: {list(df.columns)}")
    return df


def _norm(s: str) -> str:
    """Lowercase, normalise unicode dashes → ASCII hyphen, collapse whitespace."""
    s = unicodedata.normalize("NFKD", s)
    s = re.sub(r"[‐-―−﹘﹣－‒–—]", "-", s)
    return re.sub(r"\s+", " ", s.lower()).strip()


def _col(df: pd.DataFrame, *alts: str) -> str | None:
    alts_n = {_norm(a) for a in alts}
    # 1. Exact normalised match
    for c in df.columns:
        if _norm(c) in alts_n:
            return c
    # 2. Substring: every significant word of an alternative appears in the column name
    for c in df.columns:
        cn = _norm(c)
        for a in alts_n:
            words = [w for w in a.split() if len(w) > 3]
            if words and all(w in cn for w in words):
                return c
    return None


def _clean(x) -> str:
    return "" if x is None else str(x).strip()


def classify_fuel(primary: str, tech: str) -> str:
    p, t = _clean(primary).upper(), _clean(tech).upper()
    if "BATTERY" in p or "BATTERY" in t:               return "Battery"
    if "SOLAR" in p or "SOLAR" in t or "PV" in t:      return "Solar - Utility"
    if "WIND" in p or "WIND" in t:                     return "Wind"
    if "WATER" in p or "HYDRO" in p or "HYDRO" in t:   return "Hydro"
    if "COAL" in p:
        return "Brown Coal" if "BROWN" in p else "Black Coal"
    if "GAS" in p or "METHANE" in p:
        if any(k in t for k in ("COMBINED CYCLE", "CCGT")):        return "Gas - CCGT"
        if any(k in t for k in ("OPEN CYCLE", "OCGT", "ENGINE",
                                 "RECIPROCATING", "PEAKER")):       return "Gas - OCGT/Peaker"
        if "STEAM TURBINE" in t:                                    return "Gas - OCGT/Peaker"
        return "Gas - Other"
    if any(k in p for k in ("BAGASSE", "BIOMASS", "LANDFILL", "SEWAGE", "WASTE GAS", "BIOGAS")):
        return "Bioenergy"
    if any(k in p for k in ("DIESEL", "DISTILLATE", "KEROSENE")):  return "Liquid Fuel"
    return "Other"


def build_mapping(df_raw: pd.DataFrame) -> pd.DataFrame:
    station_col = _col(df_raw, "Station Name", "Station", "Power Station Name", "Power Station")
    region_col  = _col(df_raw, "Region", "RegionID", "Region Id", "Region ID")
    fuel_col    = _col(df_raw, "Fuel Source - Primary", "Fuel Source – Primary",
                               "Fuel Source - Descriptor", "Fuel Source – Descriptor", "Fuel")
    tech_col    = _col(df_raw, "Technology Type - Primary", "Technology Type – Primary",
                               "Technology Type - Descriptor", "Technology Type – Descriptor", "Technology")

    print(f"Columns resolved → station={station_col!r}  region={region_col!r}  "
          f"fuel={fuel_col!r}  tech={tech_col!r}")

    if fuel_col is None:
        print("WARNING: fuel column not found — all units will be classified as 'Other'. "
              "Manual overrides will still apply.")

    fuels = [
        classify_fuel(
            df_raw.at[i, fuel_col] if fuel_col else "",
            df_raw.at[i, tech_col] if tech_col else "",
        )
        for i in df_raw.index
    ]

    out = pd.DataFrame({
        "DUID":    df_raw["DUID"].astype(str).str.upper().str.strip(),
        "Station": df_raw[station_col].astype(str).str.strip() if station_col else "",
        "Fuel":    pd.Series(fuels, index=df_raw.index),
        "Region":  df_raw[region_col].astype(str).str.upper().str.strip() if region_col else "",
    })
    out = (out[out["DUID"].str.strip() != ""]
           .drop_duplicates(subset=["DUID"], keep="last")
           .sort_values("DUID")
           .reset_index(drop=True))

    # Apply manual overrides (persists correct fuel even if AEMO workbook column matching fails)
    applied = 0
    for idx, row in out.iterrows():
        duid = row["DUID"]
        if duid in MANUAL_OVERRIDES:
            out.at[idx, "Fuel"] = MANUAL_OVERRIDES[duid]
            applied += 1
    print(f"Manual overrides applied: {applied}")
    return out


def main():
    url = find_registration_workbook_url()
    print("Workbook URL:", url)
    content = download_workbook(url)
    raw = read_generators_sheet(content)
    mapping = build_mapping(raw)
    OUT_CSV.write_text(mapping.to_csv(index=False), encoding="utf-8")
    print(f"\nWrote {len(mapping)} rows → {OUT_CSV}")
    print("\nDUID count by Fuel:")
    print(mapping["Fuel"].value_counts().to_string())


if __name__ == "__main__":
    main()
