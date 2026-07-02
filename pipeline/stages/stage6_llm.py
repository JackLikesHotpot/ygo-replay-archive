import os
import json
import time
import pandas as pd
from pathlib import Path
from prefect import task
from groq import Groq
from dotenv import load_dotenv
load_dotenv()

CHECKPOINT_DIR = Path("checkpoints")

PROMPT_TEMPLATE = """
You are a Yu-Gi-Oh! feature match classification system.
Your job is to analyze an input JSON array of videos and return a JSON array of results. 

For EVERY video object in the input array, return EXACTLY ONE object in the output array in the same order.

CRITICAL RULES FOR EXTRACTION:
1. AUTOMATIC MATCH DETECTION: If the "title" or "description" contains ANY of these patterns, you MUST set "is_match" to true:
   - "Round " followed by a number (e.g., "Round 5", "Round 9")
   - "Top " followed by a number (e.g., "Top 8")
   - "Final", "Finals", "Semi-Final", "Quarterfinal"
   - "vs" or "vs."
   - "Feature Match"
2. If "is_match" is true, extract the fields from the title/description. Do not guess; use null if unknown.
3. If "is_match" is false, set all fields except "video_id" to null.

Output Schema:
[
  {
    "video_id": "string",
    "is_match": true or false,
    "tournament": "string or null",
    "round": "string or null (Format exactly as: 'Round X', 'Top X', 'Finals', 'Semi-Final')",
    "player_1": "string or null",
    "deck_1": "string or null",
    "player_2": "string or null",
    "deck_2": "string or null"
  }
]

RESPONSE SYSTEM RULES:
- DO NOT write any introductory or concluding text.
- DO NOT wrap the output in markdown code blocks (no ```json).
- Your entire response must be a single valid JSON array starting with [ and ending with ].

INPUT:
"""


def safe_parse_json(content: str):
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
        content = content.strip()

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        # try to salvage truncated JSON
        for suffix in ["]", "}]", "}}"]:
            try:
                return json.loads(content + suffix)
            except json.JSONDecodeError:
                continue

        print(f"JSON parse failed — could not salvage")
        print(f"Raw content (last 300 chars): {content[-300:]}")
        return None

import re

# Comprehensive tournament pattern regex
MANDATORY_MATCH_RE = re.compile(
    r'\bround\s*\d+\b|\btop\s*\d+\b|\bfinal(s)?\b|\bplayoff\b|\bvs\.?|\bfeature\s*match\b', 
    re.IGNORECASE
)

@task(name="Classify feature matches with LLM")
def classify_matches(videos_df: pd.DataFrame, chunk_size: int = 10, sleep_seconds: int = 30) -> pd.DataFrame:
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    llm_input = videos_df[["Video ID", "Title", "Video Description"]].copy()
    llm_input["Video Description"] = llm_input["Video Description"].str[:200]
    rows = llm_input.to_dict("records")

    # ── Resume from existing progress ────────────────────────────────
    progress_path = CHECKPOINT_DIR / "llm_progress.parquet"
    if progress_path.exists():
        existing = pd.read_parquet(progress_path)
        done_ids = set(existing["video_id"])
        rows = [r for r in rows if r["Video ID"] not in done_ids]
        all_results = existing.to_dict("records")
        print(f"Resuming — {len(done_ids)} already done, {len(rows)} remaining")
    else:
        all_results = []

    total_chunks = -(-len(rows) // chunk_size)

    for i in range(0, len(rows), chunk_size):
        chunk_id = i // chunk_size + 1
        chunk = rows[i:i + chunk_size]
        print(f"Processing chunk {chunk_id}/{total_chunks}")

        chunk_prompt = PROMPT_TEMPLATE + json.dumps(chunk, ensure_ascii=False)

        try:
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[{"role": "user", "content": chunk_prompt}],
                temperature=0,
                max_tokens=1500,
            )
            content = response.choices[0].message.content.strip()
            parsed = safe_parse_json(content)

            if parsed is not None:
                if isinstance(parsed, dict):
                    for val in parsed.values():
                        if isinstance(val, list):
                            parsed = val
                            break

                if isinstance(parsed, list):
                    # ── PYTHON PATTERN ENFORCEMENT OVERRIDE ──────────────────
                    for parsed_item, original_item in zip(parsed, chunk):
                        # Handle potential None elements safely
                        title = str(original_item.get("Title") or "")
                        desc = str(original_item.get("Video Description") or "")
                        full_text = f"{title} {desc}"
                        
                        # Force is_match to True if a clear match token exists
                        if MANDATORY_MATCH_RE.search(full_text):
                            parsed_item["is_match"] = True
                            
                            # Fallback metadata if LLM choked on empty values
                            if not parsed_item.get("tournament") and "ycs" in title.lower():
                                parsed_item["tournament"] = "YCS Match"
                    # ──────────────────────────────────────────────────────────

                    all_results.extend(parsed)
                    print(f"   ✓ {len(parsed)} rows returned")
                else:
                    print(f"   ✗ chunk {chunk_id} structural anomaly: expected list")
            else:
                print(f"   ✗ chunk {chunk_id} failed — skipping")

        except Exception as e:
            error_str = str(e)
            if "rate_limit_exceeded" in error_str and "tokens per day" in error_str:
                print(f"   ✗ Daily token limit hit — stopping. Resume later.")
                break
            else:
                print(f"   ✗ chunk {chunk_id} API error: {e}")

        if all_results:
            CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(all_results).to_parquet(progress_path, index=False)

        if i + chunk_size < len(rows):
            time.sleep(sleep_seconds)

    results_df = pd.DataFrame(all_results)
    
    # Post-processing deduplication
    initial_count = len(results_df)
    results_df = results_df.drop_duplicates(subset=["video_id"], keep="first")
    dropped_count = initial_count - len(results_df)
    if dropped_count > 0:
        print(f"--> Cleaned up {dropped_count} duplicate rows hallucinated by the LLM.")

    # Ensure one row per video in metadata mapping
    video_meta = videos_df.drop_duplicates(subset=["Video ID"])[
        ["Video ID", "Video Description", "Playlist ID", "Playlist Title", "Title", "Clean Title", "Category", "Year"]
    ]

    results_df = results_df.merge(
        video_meta,
        left_on="video_id",
        right_on="Video ID",
        how="left"
    ).drop(columns=["video_id"]) # Drop original join key to prevent duplication

    # Suffix cleanup
    duplicate_cols = [col for col in results_df.columns if col.endswith('_y')]
    results_df = results_df.drop(columns=duplicate_cols)
    results_df = results_df.rename(columns=lambda x: x.rstrip('_x'))
    
    print(f"\nTotal rows processed: {len(results_df)} / {len(llm_input)}")
    return results_df


def run(videos_df: pd.DataFrame, resume: bool = True) -> pd.DataFrame:
    checkpoint = CHECKPOINT_DIR / "stage6_matches.parquet"
    if resume and checkpoint.exists():
        print("--- Loading matches from checkpoint ---")
        return pd.read_parquet(checkpoint)

    result = classify_matches(videos_df)

    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    result.to_parquet(checkpoint, index=False)

    return result