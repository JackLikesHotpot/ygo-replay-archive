import pandas as pd
import os
from prefect import task

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


@task(name="Flag unknown fields")
def flag_unknowns(df: pd.DataFrame) -> pd.DataFrame:
    p1 = df["player_1"].astype(str).str.strip().str.lower()
    p2 = df["player_2"].astype(str).str.strip().str.lower()
    d1 = df["deck_1"].astype(str).str.strip().str.lower()
    d2 = df["deck_2"].astype(str).str.strip().str.lower()

    mask = (
        (p1 == "unknown") |
        (p2 == "unknown") |
        (d1 == "unknown") |
        (d2 == "unknown")
    )

    unknowns = df[mask].copy()
    out_path = os.path.join(OUTPUT_DIR, "unknowns.csv")
    unknowns.to_csv(out_path, index=False)
    print(f"Flagged {len(unknowns)} rows with unknown fields → {out_path}")
    return unknowns


def run(matches_df: pd.DataFrame) -> pd.DataFrame:
    return flag_unknowns(matches_df)