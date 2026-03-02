from pathlib import Path
import pandas as pd
import os


data_dir = Path(os.path.join(os.path.dirname(__file__), "..", "data"))

MAPPING = data_dir / "static" / "duid_fuel.csv"

CURATED_SCADA = data_dir / "curated" / "scada"



def load_mapping() -> pd.DataFrame:
    return pd.read_csv(MAPPING, comment="#", dtype=str).fillna("").assign(DUID=lambda d: d["DUID"].str.upper().str.strip())

def list_scada_duids() -> pd.DataFrame:
    files = sorted(CURATED_SCADA.glob("scada_*.parquet"))
    
    if not files:
        raise SystemExit("No SCADA parquet files found. Run SCADA backfill first")
    duids = []
    for f in files:
        df = pd.read_parquet(f, columns=["DUID"])
        duids.append(df["DUID"].astype(str).str.upper().str.strip())
    return pd.concat(duids).dropna().unique()


if __name__ == "__main__":
    map_df = load_mapping()
    known = set(map_df["DUID"].tolist())
    scada_duids = list_scada_duids()

    unknown = [d for d in scada_duids if d not in known]
    print(f"Total SCADA DUIDS: {len(scada_duids)}")
    print(f"Known in mapping: {len(known)}")
    print(f"Unknown: {len(unknown)}")

    if unknown:
        print("\nUnknown DUIDS (add rows to data/static/duid_fuel.csv):")
        for d in sorted(unknown):
            print(f"{d},,,")
    else:
        print("\nAll DUIDS are mapped.")   