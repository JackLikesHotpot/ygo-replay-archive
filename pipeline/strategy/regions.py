# regions.py
from dataclasses import dataclass, field
from typing import Callable, Literal
import re
import pandas as pd

@dataclass
class RegionStrategy:
    name: str
    source_type: Literal["playlists", "videos"]
    filter_phrases: list[str]
    exclude_phrases: list[str]
    extract_metadata: Callable[[pd.Series], pd.Series]
    extra_filters: list[Callable[[pd.DataFrame], pd.DataFrame]] = field(default_factory=list)

    def filter_playlists(self, df: pd.DataFrame) -> pd.DataFrame:
        pattern = "|".join(self.filter_phrases)
        mask = df["title"].str.contains(pattern, case=False, na=False)
        df = df[mask]
        for extra in self.extra_filters:
            df = extra(df)
        return df