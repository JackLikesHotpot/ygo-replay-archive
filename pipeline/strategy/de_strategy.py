# de_strategy.py
from regions import RegionStrategy
import re, pandas as pd

DE_PHRASES = ["Deutsche Meisterschaft", "YCS", "WCQ", "German Championship"]

def de_extract_metadata(row: pd.Series) -> pd.Series:
    # DE-specific regex
    ...

DE = RegionStrategy(
    name="DE",
    source_type="playlists",
    filter_phrases=DE_PHRASES,
    extract_metadata=de_extract_metadata,
)