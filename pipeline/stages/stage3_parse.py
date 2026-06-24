import re 
import pandas as pd
from prefect import task

# Global Regex Compilation Matrix
RE_EURO = re.compile(r"(?:WCQ:\s*)?\bEuropean\b(?:\s+Yu-Gi-Oh!)?\s+\bChampionship\b", re.IGNORECASE)
RE_NATS = re.compile(r"WCQ:\s*([A-Za-z]+)\s+National Championship", re.IGNORECASE)
RE_OPEN = re.compile(r"\b([A-Za-z]+)\s+Open\b", re.IGNORECASE)
RE_YEARS = re.compile(r'\b(19\d{2}|20\d{2})\b')

def clean_base_text(full_title):
    """Handles pipe splitting, string type conversions, and leading/trailing whitespace."""
    base_text = str(full_title).strip()
    if "|" in base_text:
        base_text = base_text.split("|")[0].strip()
    return base_text


def extract_tournament_year(text):
    """Finds the main tournament year and strips it out of the title string."""
    year_match = RE_YEARS.search(text)
    if year_match:
        year = year_match.group(1)
        clean_title = text.replace(year, "").replace("  ", " ").strip(" -:")
        return year, clean_title
    return "Unknown", text


def process_ycs_event(clean_title, base_text):
    """Applies specialized transformations for YCS, Remote Duels, and Time Wizard formats."""
    category = 'Advanced'
    is_remote = 'Remote Duel' in clean_title
    
    if any(kw in clean_title for kw in ['Time Wizard', 'Ultimate Time Wizard']) or any(fmt in base_text for fmt in ['Edison', 'Goat', 'Toss', 'HAT']):
        category = 'Time Wizard'
        clean_title = clean_title.replace('Ultimate Time Wizard', '').replace('Time Wizard', '').replace('Format', '')
        clean_title = RE_YEARS.sub('', clean_title)  # Wipe retro format years (e.g., 2010)

    if 'Genesys' in clean_title:
        category = 'Genesys'
        
    clean_title = clean_title.replace('Yu-Gi-Oh! Championship Series', 'YCS')

    if 'YCS' in clean_title:
        clean_title = 'YCS' + clean_title.split('YCS')[1]
        clean_title = re.sub(r'\bYCS\s+in\s+', 'YCS ', clean_title, flags=re.IGNORECASE)
        
    if is_remote:
        clean_title = 'Remote ' + clean_title

    if any(dash in clean_title for dash in ['–', '-']) and 'Top' in clean_title:
        split_char = '–' if '–' in clean_title else '-'
        parts = clean_title.split(split_char)
        clean_title = f"{parts[0].strip()} ({parts[1].strip()})"
        
    clean_title = re.sub(r'\s+', ' ', clean_title).strip(" -")
    return clean_title, category


def identify_other_categories(clean_title):
    """Checks for World Championships, Nationals, Opens, and Continental WCQs."""
    category = 'Advanced'
    
    # 1. World Championships
    if 'World Championship' in clean_title or 'WCS' in clean_title:
        if 'Dragon Duel' in clean_title:
            category = 'Dragon Duel'
        return 'World Championship', category
    
    # 2. National Championships
    if match_nat := RE_NATS.search(clean_title):
        country = match_nat.group(1).strip()
        return f"{country} National Championship", category
        
    # 3. Open Tournaments
    if match_open := RE_OPEN.search(clean_title):
        country = match_open.group(1).strip()
        country = 'UK' if country.lower() == 'uk' else country.capitalize()
        return f"{country} Open", category
        
    # 4. Continental WCQs
    if 'WCQ: EC' in clean_title or 'World Championship Qualifier' in clean_title or RE_EURO.search(clean_title):
        return 'European WCQ', category
        
    return clean_title, category


def extract_title_and_year(full_title):
    if 'interview' in str(full_title).lower():
        return pd.Series([None, None, None])
        
    base_text = clean_base_text(full_title)
    year, clean_title = extract_tournament_year(base_text)
    if 'Yu-Gi-Oh! Championship Series' in clean_title or 'YCS' in clean_title:
        clean_title, category = process_ycs_event(clean_title, base_text)
    else:
        clean_title, category = identify_other_categories(clean_title)
        
    return pd.Series([clean_title, year, category])

@task(name="Parse playlist titles")
def parse_titles(df: pd.DataFrame) -> pd.DataFrame:
    df[["Clean Title", "Year", "Category"]] = df["Playlist Title"].apply(extract_title_and_year)
    df = df.dropna(subset=["Clean Title"])
    print(f"Parsed {len(df)} playlists after dropping interviews.")
    return df[["Clean Title", "Year", "Category", "Video Count", "Playlist ID", "Playlist Title"]]

def run(filtered_df: pd.DataFrame) -> pd.DataFrame:
    return parse_titles(filtered_df)