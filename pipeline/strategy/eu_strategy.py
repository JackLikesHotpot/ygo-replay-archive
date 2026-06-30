# eu_strategy.py
import re, pandas as pd
from strategy.regions import RegionStrategy

EU_PHRASES = ["YCS", "Championship", "WCQ", "National", "Open"]

def eu_extract_metadata(row: pd.Series) -> pd.Series:
    pattern = r"(?P<name>YCS|WCQ|National|Open[^|]+)\|?\s*(?P<year>\d{4})"
    match = re.search(pattern, row["title"])
    return pd.Series({
        "tournament_name": match.group("name").strip() if match else None,
        "year":            match.group("year") if match else None,
        "category":        "World Championship" if "WCQ" in row["title"] else "Advanced",
        "video_count":     row.get("video_count"),
        "playlist_id":     row["playlist_id"],
    })

EU = RegionStrategy(
    name="EU",
    source_type="playlists",
    filter_phrases=["YCS", "Championship", "WCQ", "National", "Open"],
    exclude_phrases=["Interview", "Registration", "Games"],
    extract_metadata=eu_extract_metadata,
)