import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock

import pandas as pd
from google import genai
from google.genai import types
from prefect import task
from pydantic import BaseModel

CHECKPOINT = "checkpoints/stage5_llm.parquet"

class MatchRecord(BaseModel):
    is_match: bool
    tournament: str
    round: str
    player_1: str
    deck_1: str
    player_2: str
    deck_2: str

def _gemini_parse(client, title: str, description: str) -> dict:
    prompt = f"""
    You are an expert tournament data extractor for the Yu-Gi-Oh! TCG. 
    Analyze the video title and description to extract match data.
    
    ### CRITICAL FILTERING RULES:
    1. A video is a valid match (`is_match`: true) ONLY if it is an official, standard multi-player tournament match featuring two active players and an explicitly stated round or top-cut designation (e.g., Round 1, Top 8, Finals, Feature Match).
    2. Set `is_match`: false if the video is an interview, deck profile, vlog, highlight reel, or casual discussion.
       
    ### DATA EXTRACTION RULES (Only if `is_match` is true):
    - Search BOTH the TITLE and DESCRIPTION to extract fields. If a field cannot be found anywhere, use "Unknown". Do not guess.
    - Match metadata like "Feature Match:" or "Round X" should be extracted into `round` and `tournament`, not left inside player names.
    - Remove all flag emojis, country bracket prefixes (e.g., [DE], [UK]), and trailing syntax from player names.

    ### PARENTHESES & DECK CLEANING RULES (CRITICAL):
    - Decks are usually listed in parentheses next to player names: "Player Name (Deck)". 
    - Handle MALFORMED or UNCLOSED parenthesis aggressively! If a line break or formatting error splits a deck name (e.g., "(Majespecter Pendulum\n)"), merge it, strip the newline, and correctly extract "Majespecter Pendulum" as the deck.
    - If a player has a deck listed next to them, map them strictly as a pair:
      Example: "Samir Bacher (Majespecter Pendulum) vs. Sven-Ole Wegner (XYZ Monarchs)"
      -> player_1: "Samir Bacher", deck_1: "Majespecter Pendulum"
      -> player_2: "Sven-Ole Wegner", deck_2: "XYZ Monarchs"
    - If the title contains only tournament names (e.g., "YCS Bochum 2018: Round 2 Feature Match") but the description lists the actual player pairings and decks, extract the players and decks from the description.

    TITLE: {title}
    DESCRIPTION: {description}
    """

    response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents=prompt,
    config=types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=MatchRecord,
        temperature=0.1,
      ),
    )
    return json.loads(response.text)

def _process_single_video(client, entry: dict, print_lock: Lock) -> dict | None:
    title   = entry.get("Title", "")
    desc    = entry.get("Video Description", "")
    pid     = entry.get("Playlist ID", "Unknown")
    snippet = title[:40]

    with print_lock:
      print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting: {snippet}...")

    try:
        result = _gemini_parse(client, title, desc)
        if result.get("is_match"):
            result["playlist_id"]    = pid
            result["original_title"] = title
            with print_lock:
                print(f"   ✅ {result['player_1']} ({result['deck_1']}) vs "
                      f"{result['player_2']} ({result['deck_2']})")
            return result
        else:
            with print_lock:
                print(f"   ⏩ Skipped: {snippet}...")
            return None
    except Exception as e:
        with print_lock:
            print(f"   ❌ Error on '{snippet}': {e}")
        if "429" in str(e):
            time.sleep(30)
        return None
    

@task(name="LLM match tagging")
def tag_matches(videos_df: pd.DataFrame) -> pd.DataFrame:
    client  = genai.Client()
    lock    = Lock()
    records = videos_df.to_dict("records")
    results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_process_single_video, client, v, lock): v for v in records}
        for future in as_completed(futures):
            data = future.result()
            if data:
                results.append(data)

    df = pd.DataFrame(results)
    print(f"Extracted {len(df)} matches from {len(records)} videos.")
    return df


def run(videos_df: pd.DataFrame, resume: bool = True) -> pd.DataFrame:
    if resume and os.path.exists(CHECKPOINT):
        print("[stage5] Resuming from checkpoint")
        return pd.read_parquet(CHECKPOINT)

    df = tag_matches(videos_df)
    df.to_parquet(CHECKPOINT, index=False)
    return df