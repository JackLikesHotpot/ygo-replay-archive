import os 
import pandas as pd
from googleapiclient.errors import HttpError
from prefect import task
from prefect.tasks import exponential_backoff

CHECKPOINT = "checkpoints/stage1_playlists.parquet"

@task(name="Resolve channel handle", retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=2))
def get_channel_id(youtube, handle: str) -> str:
  response = youtube.search().list(
    q = handle,
    type = 'channel',
    part = 'id, snippet',
    maxResults = 1
  ).execute()

  items = response.get('items', [])
  if not items:
    raise ValueError(f"Could not find channel: {handle}")
  
  channel_id = items[0]["id"]["channelId"]
  print(f"Successfully connected to '{items[0]['snippet']['title']}'.")
  return channel_id

@task(name="Fetch all playlists", retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=2))
def fetch_playlists(youtube, channel_id: str) -> pd.DataFrame:
  playlists = []
  next_page_token = None

  while True:
    response = youtube.playlists().list(
      channelId = channel_id,
      part = "snippet, contentDetails",
      maxResults = 50,
      pageToken = next_page_token
    ).execute()

    for item in response.get("items", []):
      playlists.append({
        "Playlist Title": item["snippet"]["title"],
        "Video Count": item["contentDetails"]["itemCount"],
        "Playlist ID": item["id"]
      })

    next_page_token = response.get("nextPageToken")
    if not next_page_token:
      break

  df = pd.DataFrame(playlists)
  print(f"Fetched {len(df)} playlists.")
  return df

def run(youtube, handle: str, resume: bool = True) -> pd.DataFrame:
  if resume and os.path.exists(CHECKPOINT):
    print(f"[stage1] Resuming from checkpoint")
    return pd.read_parquet(CHECKPOINT)
  
  channel_id = get_channel_id(youtube, handle)
  df = fetch_playlists(youtube, channel_id)
  df.to_parquet(CHECKPOINT, index=False)
  return df