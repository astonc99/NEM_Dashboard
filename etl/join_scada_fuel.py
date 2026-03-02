from pathlib import Path
import pandas as pd
import os

data_dir = Path(os.path.join(os.path.dirname(__file__), "..", "data"))
from util_mapping import load_duid_fuel_mapping
CURATED_SCADA = data_dir / "curated" / "scada"

def load_all_scada() -> pd.DataFrame:
    """Load and concataenate all monthly SCADA files."""
    files = sorted(CURATED_SCADA.glob("scada_*.parquet"))
    if not files:
        return pd.DataFrame(columns=["DUID", "ts_start_nem", "ts_end_nem", "mw"])
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df["DUID"] = df["DUID"].astype(str).str.upper().str.strip()
    return df

def scada_with_fuel() -> pd.DataFrame:
    scada = load_all_scada()
    if scada.empty:
        return scada.assign(Fuel=pd.Series(dtype=str), Station=pd.Series(dtype=str), Region=pd.Series(dtype=str))

    mapping = load_duid_fuel_mapping()
    merged = scada.merge(mapping, on="DUID", how="left")
    merged["Fuel"] = merged["Fuel"].fillna("UNKNOWN")
    merged["Station"] = merged.get("Station", pd.Series(index=merged.index)).fillna("")
    merged["Region"] = merged.get("Region", pd.Series(index=merged.index)).fillna("")
    return merged
