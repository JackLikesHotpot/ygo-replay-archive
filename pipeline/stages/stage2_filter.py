import pandas as pd
import re
from pathlib import Path
from prefect import task
from strategy.regions import RegionStrategy

@task(name="Filter tournaments by phrases")
def filter_tournaments(df: pd.DataFrame, strategy: RegionStrategy) -> pd.DataFrame:
    strict_regex  = "|".join([rf"\b{re.escape(word)}\b" for word in strategy.filter_phrases])
    exclude_regex = "|".join([re.escape(phrase) for phrase in strategy.exclude_phrases])

    tournaments = df[
        df["Playlist Title"].str.contains(strict_regex, case=False, na=False)
        & (~df["Playlist Title"].str.contains(exclude_regex, case=False, na=False))
    ]

    tournaments = tournaments.sort_values(by="Video Count", ascending=False)
    print(f"--- Found {len(tournaments)} playlists matching tournament phrases ---")
    return tournaments


def run(playlists_df: pd.DataFrame, strategy: RegionStrategy) -> pd.DataFrame:
    return filter_tournaments(playlists_df, strategy)