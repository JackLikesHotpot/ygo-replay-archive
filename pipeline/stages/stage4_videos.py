from googleapiclient.discovery import build
import pandas as pd
from prefect import task
import os

CHECKPOINT_DIR = "checkpoints"

@task(name='Get videos from playlists')
def get_playlist_videos(youtube, parsed_df: pd.DataFrame) -> pd.DataFrame:
    video_data = []

    for id in parsed_df["Playlist ID"].tolist():
        next_page_token = None

        while True:
            response = youtube.playlistItems().list(
                part='snippet',
                playlistId=id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in response.get('items', []):
                snippet = item.get('snippet', {})
                video_data.append({
                    'Playlist ID':       id,
                    'Video ID':          snippet.get('resourceId', {}).get('videoId', 'Unknown'),
                    'Title':             snippet.get('title', ''),
                    'Video Description': snippet.get('description', '')
                })

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

    df = pd.DataFrame(video_data)

    df = df.merge(
        parsed_df[["Playlist ID", "Clean Title", "Year", "Category", "Playlist Title"]],
        on="Playlist ID",
        how="left"
    )

    return df


def run(youtube, parsed_df: pd.DataFrame, resume: bool = True) -> pd.DataFrame:
    checkpoint = f"{CHECKPOINT_DIR}/stage4_videos.parquet"

    if resume and os.path.exists(checkpoint):
        print(f"[stage4] Resuming from checkpoint")
        return pd.read_parquet(checkpoint)

    df = get_playlist_videos(youtube, parsed_df)

    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    df.to_parquet(checkpoint, index=False)
    print(f"[stage4] Saved {len(df)} videos")
    return df