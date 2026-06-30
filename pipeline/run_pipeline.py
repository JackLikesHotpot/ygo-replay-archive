# run_pipeline.py

from prefect import flow, task
from googleapiclient.discovery import build
import os
from stages import stage1_playlists, stage2_filter, stage3_parse, stage4_videos, stage5_filter, stage6_review
from dotenv import load_dotenv
import pandas as pd
from strategy.regions import RegionStrategy
from strategy.eu_strategy import EU
from strategy.na_strategy import NA
load_dotenv()

@task
def run_channel_to_videos(youtube, handle: str, strategy: RegionStrategy, resume: bool = True) -> pd.DataFrame:
  playlists = stage1_playlists.run(youtube, handle, resume)
  filtered  = stage2_filter.run(playlists, strategy)
  parsed    = stage3_parse.run(filtered)

  # videos["region"] = strategy.name
  # return videos
  return parsed

@flow(name = "YGO Replay Analyser")
def ygo_pipeline(resume: bool = True):
    youtube = build("youtube", "v3", developerKey=os.getenv("YOUTUBE_API_KEY"))

    eu_videos = run_channel_to_videos(youtube, "@YuGiOhCardEU", EU, resume)
    na_videos = run_channel_to_videos(youtube, "@OfficialYuGiOhTCG", NA, resume)

    all_videos = pd.concat([eu_videos, na_videos], ignore_index=True)
    videos    = stage4_videos.run(youtube, all_videos)
    filter    = stage5_filter.run(videos)
    # matches = stage5_llm.run(all_videos, resume)
    # stage6_review.run(matches)

if __name__ == "__main__":
    ygo_pipeline(resume=True)