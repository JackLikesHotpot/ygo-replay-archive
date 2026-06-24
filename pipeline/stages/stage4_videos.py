from googleapiclient.discovery import build
import pandas as pd
from prefect import task
import os

CHECKPOINT = "checkpoints/stage4_videos.parquet"

@task(name='Get videos from playlists')
def get_playlist_videos(youtube, playlist_ids: list) -> pd.DataFrame:
  video_data = []

  for id in playlist_ids:
    next_page_token = None

    while True:
      response = youtube.playlistItems().list(
        part = 'snippet',
        playlistId = id,
        maxResults = 50,
        pageToken = next_page_token
      ).execute()

      for item in response.get('items', []):
        snippet = item.get('snippet', {})
        title = snippet.get('title', '')
        description = snippet.get('description', '')
        video_id = snippet.get('resourceId', {}).get('videoId', 'Unknown')

        video_data.append({
            'Playlist ID': id,
            'Video ID': video_id, 
            'Title': title,
            'Video Description': description
        })

      next_page_token = response.get('nextPageToken')
      if not next_page_token:
        break

    return pd.DataFrame(video_data)
  
def run(youtube, parsed_df: pd.DataFrame, resume: bool = True) -> pd.DataFrame:
    if resume and os.path.exists(CHECKPOINT):
      print(f"[stage1] Resuming from checkpoint")
      return pd.read_parquet(CHECKPOINT)
    
    playlists_id = parsed_df["Playlist ID"].tolist()
    df = get_playlist_videos(youtube, playlists_id)
    df.to_parquet(CHECKPOINT, index=False)
    return df