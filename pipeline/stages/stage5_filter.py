import re
import pandas as pd
from pathlib import Path
from prefect import task

CHECKPOINT_DIR = Path("checkpoints")

RE_MATCH_INDICATOR = re.compile(
    r'\bround\s*\d+|\btop\s*\d+|\bfinals?\b|\bsemi[-\s]?finals?\b|\bvs\.?\b|\b\d+(?:st|nd|rd|th)\s+place\b',
    re.IGNORECASE
)
RE_EXCLUDE = re.compile(
    r'\bannouncement\b|\binterview\b|\bdiscussion\b|\bdiscuss\b|\bvlog\b|'
    r'\bdeck\s*profile\b|\bgames\b|\bmemories\b|\bchronicles\b|\bcaster\s+vs\b|'
    r'\bduel\s+links\b|\byu-gi-oh!\s+games\b|\bhighlights\b|\bhot\s+takes\b|'
    r'\bmaster\s+vs\b|\bceremony\b|\bshowcase\b|\breal\s+life\s+duel\b|\binfluencer\b',
    re.IGNORECASE
)
RE_LIVESTREAM_DAY = re.compile(r'\bday\s*[123]\b', re.IGNORECASE)


@task(name="Filter match videos")
def filter_match_videos(df: pd.DataFrame) -> pd.DataFrame:
    match_mask   = df["Title"].str.contains(RE_MATCH_INDICATOR, na=False)
    exclude_mask = df["Title"].str.contains(RE_EXCLUDE, na=False)

    matches = df[match_mask & ~exclude_mask]
    dropped = df[~match_mask | exclude_mask]

    # split out livestreams from the dropped set
    livestream_mask = dropped["Title"].str.contains(RE_LIVESTREAM_DAY, na=False)
    livestreams = dropped[livestream_mask]
    dropped_remaining = dropped[~livestream_mask]

    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    livestreams.to_parquet(CHECKPOINT_DIR / "livestreams.parquet", index=False)
    matches.to_parquet(CHECKPOINT_DIR / "stage5_matches.parquet", index=False)

    print(f"--- Kept {len(matches)} match videos ---")
    print(f"--- Livestreams: {len(livestreams)} ---")
    print(f"--- Dropped (other): {len(dropped_remaining)} ---")

    return matches


def run(videos_df: pd.DataFrame) -> pd.DataFrame:
    return filter_match_videos(videos_df)