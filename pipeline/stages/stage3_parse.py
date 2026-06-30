import re
import pandas as pd
from prefect import task
from pathlib import Path

CHECKPOINT_DIR = Path("checkpoints")

# REGEX

RE_YEAR = re.compile(r'(19\d{2}|20\d{2})\b')

RE_YCS = re.compile(r'\bYCS\b', re.IGNORECASE)
RE_WORLDS = re.compile(r'\bWorld\s+Championship\b|\bWCS\b', re.IGNORECASE)
RE_WCQ = re.compile(r'WCQ\b|\bWorld\s+Qualifying\s+Points\b|\bEuropean\s+(?:Yu[\s-]?Gi[\s-]?Oh!?\s*)?Championship\b', re.IGNORECASE)
RE_WQP = re.compile(r'\bWQP\b', re.IGNORECASE)
RE_NATIONAL = re.compile(r'\b([A-Za-z]+)\s+National\b|\bNational\s+Championship\b', re.IGNORECASE)
RE_OPEN = re.compile(r'\b([A-Za-z]+)\s+Open\b', re.IGNORECASE)

RE_REMOTE = re.compile(r'\bRemote\s+Duel\b', re.IGNORECASE)
RE_DRAGON = re.compile(r'\bDragon\s+Duel\b', re.IGNORECASE)

RE_DUELING_ARCHIVES = re.compile(r'^\s*Dueling\s+Archives\s*[:\-]?\s*', re.IGNORECASE)
RE_NOISE_PREFIX = re.compile(r'^(?:Yu-Gi-Oh!\s*)?\d+(?:st|nd|rd|th)\s+', re.IGNORECASE)
RE_SPLIT_NOISE = re.compile(r'\s*[\|\–—\-,:]\s*')
RE_NA_WCQ_LONGFORM = re.compile(r'\bNorth\s+American?\s+WCQ\b', re.IGNORECASE)
RE_YUGIOH_PREFIX = re.compile(r'^Yu[\s-]?Gi[\s-]?Oh!?\s*', re.IGNORECASE)
RE_IN_CITY = re.compile(r'\b(YCS|WCQ)\s+in\s+', re.IGNORECASE)
RE_WCS_STANDALONE = re.compile(r'\bWCS\b', re.IGNORECASE)
RE_EU_WCQ_VARIANTS = re.compile(
    r'\bWCQ\s+EC\b|\bWCQ\s+European\s+Championship\b|\bEuropean\s+World\s+Championship\s+Qualifier\b|\bEuropean\s+(?:Yu[\s-]?Gi[\s-]?Oh!?\s*)?Championship\b',
    re.IGNORECASE
)
RE_WQP_STANDALONE = re.compile(r'\bWQP\b|\bWGP\b', re.IGNORECASE)
RE_BRACKET_COUNTRY = re.compile(r'^\[\w{2}\]\s*', re.IGNORECASE)
RE_WC_QUALIFIER = re.compile(r'\bWorld\s+Championship\s+Qualifier\b', re.IGNORECASE)
RE_TCG_NOISE = re.compile(r'\b(?:TRADING\s+CARD\s+GAME|TCG)\s+', re.IGNORECASE)
RE_TRAILING_PARENS = re.compile(r'\s*\([^)]*\)\s*$')
RE_WCQ_NATIONAL_PREFIX = re.compile(r'\bWCQ\s+(?=\w+\s+National\s+Championship)', re.IGNORECASE)
# CLEANING FUNCTIONS

def strip_year(title: str):
    m = RE_YEAR.search(title)
    if m:
        return m.group(1), RE_YEAR.sub("", title).strip()
    return "Unknown", title


def clean_base(title: str) -> str:
    title = str(title).strip()
    if "|" in title:
        title = title.split("|")[0]
    return title.strip()

def normalize_remote(title: str) -> str:
    if RE_REMOTE.search(title):
        return "Remote Duel Invitational"
    return title

def normalize_ycs(title: str) -> str:
    title = RE_NOISE_PREFIX.sub("", title)

    title = re.sub(
        r"Yu-Gi-Oh!\s*Championship\s*Series",
        "YCS",
        title,
        flags=re.IGNORECASE
    )

    if "YCS" in title:
        title = "YCS" + title.split("YCS", 1)[1]

    title = RE_SPLIT_NOISE.sub(" ", title)
    return re.sub(r"\s+", " ", title).strip()

def normalize_wcq_variants(title: str, was_dueling_archives: bool = False) -> str:
    title = RE_WQP_STANDALONE.sub("WQP", title)   # normalize WGP typo -> WQP, but keep distinct from WCQ
    title = RE_NA_WCQ_LONGFORM.sub("NA World Championship", title)
    title = RE_EU_WCQ_VARIANTS.sub("EU WCQ", title)
    title = RE_WC_QUALIFIER.sub("WCQ", title)
    title = RE_WCQ_NATIONAL_PREFIX.sub("", title)

    if was_dueling_archives and re.search(r'\bWCQ\b', title, re.IGNORECASE) and "NA WORLD CHAMPIONSHIP" not in title.upper():
        title = re.sub(r'\bWCQ\b', "NA World Championship", title, flags=re.IGNORECASE)

    return title

def normalize_phrasing(title: str, was_dueling_archives: bool = False) -> str:
    title = RE_YUGIOH_PREFIX.sub("", title)
    title = RE_WQP_STANDALONE.sub("WQP", title)
    title = RE_IN_CITY.sub(r"\1 ", title)
    title = RE_WCS_STANDALONE.sub("World Championship", title)
    title = RE_BRACKET_COUNTRY.sub("", title)
    title = RE_TCG_NOISE.sub("", title)
    title = RE_NA_WCQ_LONGFORM.sub("NA World Championship Qualifer", title)
    title = RE_EU_WCQ_VARIANTS.sub("EU World Championship Qualifer", title)
    title = RE_WC_QUALIFIER.sub("WCQ", title)
    title = RE_WCQ_NATIONAL_PREFIX.sub("World Qualifying Points", title)
    title = RE_TRAILING_PARENS.sub("", title).strip()

    if was_dueling_archives and re.search(r'\bWCQ\b', title, re.IGNORECASE) and "NA WORLD CHAMPIONSHIP" not in title.upper():
        title = re.sub(r'\bWCQ\b', "NA World Championship", title, flags=re.IGNORECASE)

    return title

# CLASSIFICATION

def classify(title: str) -> str:
    if RE_DRAGON.search(title):
        return "Dragon Duel"
    if RE_REMOTE.search(title):
        return "Remote Duel Invitational"
    if RE_WQP.search(title):
        return "WQP"
    if RE_YCS.search(title):
        return "YCS"
    if RE_WORLDS.search(title):
        return "World Championship"
    if RE_WCQ.search(title):
        return "WCQ"
    if RE_NATIONAL.search(title):
        return "National Championship"
    if RE_OPEN.search(title):
        return "Open Tournament"
    return "Other"


def process_title(full_title: str):
    if "interview" in str(full_title).lower():
        return pd.Series([None, None, None])

    was_dueling_archives = bool(RE_DUELING_ARCHIVES.search(full_title))

    base = clean_base(full_title)
    year, title = strip_year(base)
    title = normalize_remote(title)
    title = normalize_ycs(title)
    title = normalize_phrasing(title, was_dueling_archives)
    title = RE_DUELING_ARCHIVES.sub("", title).strip()
    title = re.sub(r"\s+", " ", title).strip(" -:")

    category = classify(title)
    clean_title = title

    return pd.Series([clean_title, year, category])

@task(name="Parse playlist titles")
def parse_titles(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    out = df.copy()
    out.columns = ["Playlist Title", "Video Count", "Playlist ID"]

    parsed = out["Playlist Title"].apply(process_title)
    out[["Clean Title", "Year", "Category"]] = parsed
    out = out.dropna(subset=["Clean Title"])

    result = out[[
        "Clean Title",
        "Year",
        "Category",
        "Video Count",
        "Playlist ID",
        "Playlist Title"
    ]]

    if verbose:
        print(f"--- Parsed {len(result)} playlists ---")

    return result


def run(filtered_df: pd.DataFrame) -> pd.DataFrame:
    return parse_titles(filtered_df)