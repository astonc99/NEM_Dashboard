# scripts/fuel_coverage.py
from pathlib import Path
import sys
import pandas as pd
# Add the project root (parent of 'etl') to sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))


from etl.join_scada_fuel import scada_with_fuel

if __name__ == "__main__":
    df = scada_with_fuel()
    if df.empty:
        raise SystemExit("No SCADA loaded.")

    # 5-minute energy MWh approx = MW * 5/60
    df["mwh"] = df["mw"] * (5.0 / 60.0)
    cov = (
        df.groupby("Fuel", as_index=False)["mwh"]
        .sum()
        .sort_values("mwh", ascending=False)
    )
    total = cov["mwh"].sum()
    cov["share_pct"] = 100 * cov["mwh"] / (total if total > 0 else 1)
    print(cov.to_string(index=False))

    unknown_pct = (
        cov.loc[cov["Fuel"] == "UNKNOWN", "share_pct"].sum()
        if "UNKNOWN" in cov["Fuel"].values
        else 0.0
    )
    print(f"\nUNKNOWN fuel share by energy: {unknown_pct:.2f}%")
