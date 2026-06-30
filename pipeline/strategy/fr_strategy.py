# fr_strategy.py
from regions import RegionStrategy
import re, pandas as pd

FR_PHRASES = ["Championnat", "YCS", "WCQ", "Nationale"]

def fr_extract_metadata(row: pd.Series) -> pd.Series:
    # FR-specific regex on video titles directly
    ...

FR = RegionStrategy(
    name="FR",
    source_type="videos",    # pipeline will skip playlist steps
    filter_phrases=FR_PHRASES,
    extract_metadata=fr_extract_metadata,
)