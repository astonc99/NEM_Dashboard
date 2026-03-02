from pathlib import Path
import pandas as pd
import os

data_dir = Path(os.path.join(os.path.dirname(__file__), "..", "data"))

MAPPING_PATH = data_dir / "static" / "duid_fuel.csv"

def load_duid_fuel_mapping() -> pd.DataFrame:
    """
    Load the DUID→Fuel mapping CSV, ignoring commented lines.
    Ensures DUID is uppercase and trims whitespace.
    """
    df = pd.read_csv(MAPPING_PATH, comment="#", dtype=str).fillna("")
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    if "DUID" not in df.columns or "Fuel" not in df.columns:
        raise ValueError("duid_fuel.csv must contain 'DUID' and 'FUEL' columns.")
    df["DUID"] = df["DUID"].str.upper()
    return df[["DUID","Station","Fuel","Region"] if "Station" in df.columns and "Region" in df.columns else ["DUID","Fuel"]]
    

    
    