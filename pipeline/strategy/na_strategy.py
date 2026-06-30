# na_strategy.py
import re, pandas as pd
from strategy.regions import RegionStrategy

NA_PHRASES = ["YCS", "Championship", "WCQ", "Remote Duel", "Dueling Archives"]

def na_extract_metadata(row: pd.Series) -> pd.Series:
    # more complex patterns go here
    ...

NA = RegionStrategy(
    name="NA",
    source_type="playlists",
    filter_phrases=["YCS", "Championship", "WCQ", "Remote Duel", "Dueling Archives"],
    exclude_phrases=["Duel Links", "Dueling Archives: Scripted Duels"],
    extract_metadata=na_extract_metadata,
)