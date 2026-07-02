import pandas as pd
from prefect import task
from pathlib import Path

    
CHECKPOINT_DIR = Path("checkpoints")

@task(name="Flag unknown fields")
def merge_df(st5: pd.DataFrame, st6: pd.DataFrame) -> pd.DataFrame:
    
    st5_clean = st5.drop_duplicates(subset=['Video ID'])
    st6_clean = st6.drop_duplicates(subset=['video_id'])

    merged = st6_clean.merge(
        st5_clean,
        left_on="video_id",
        right_on="Video ID",
        how="left"
    )

    checkpoint = CHECKPOINT_DIR / "stage7_clean.parquet"

    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(checkpoint, index=False)
    return merged

def run(st5: pd.DataFrame, st6: pd.DataFrame) -> pd.DataFrame:
    return merge_df(st5, st6)