import pandas as pd
from prefect import task
from prefect.tasks import exponential_backoff

TARGET_PHRASES = ["YCS", "Championship", "WCQ", "National", "Open"]
strict_regex = "|".join([rf"\b{word}\b" for word in TARGET_PHRASES])

@task(name="Filter tournaments by phrases")
def filter_tournaments(df: pd.DataFrame) -> pd.DataFrame:
  tournaments = df[df["Playlist Title"].str.contains(strict_regex, case=False, na=False)]
  print(f"--- Found {len(tournaments)} playlists matching your target tournament phrases ---")
  tournaments = tournaments.sort_values(by="Video Count", ascending=False)
  return tournaments

def run(playlists_df: pd.DataFrame) -> pd.DataFrame:
  filtered = filter_tournaments(playlists_df)
  return filtered