# run_pipeline.py

from prefect import flow
from googleapiclient.discovery import build
import os
from stages import stage1_playlists, stage2_filter, stage3_parse, stage4_videos, stage5_llm, stage6_review
from dotenv import load_dotenv
load_dotenv()

@flow(name = "YGO Replay Analyser")
def ygo_pipeline(resume: bool = True):
  youtube = build("youtube", "v3", developerKey = os.getenv('YOUTUBE_API_KEY'))

  playlists   = stage1_playlists.run(youtube, "@YuGiOhCardEU", resume)
  filtered    = stage2_filter.run(playlists)
  parsed      = stage3_parse.run(filtered)
  videos      = stage4_videos.run(youtube, parsed)
  matches     = stage5_llm.run(videos, resume)
  stage6_review.run(matches)

if __name__ == "__main__":
    ygo_pipeline(resume=True)