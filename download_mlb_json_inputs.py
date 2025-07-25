import boto3
import os
from datetime import datetime

s3 = boto3.client("s3")
DATE = datetime.now().strftime("%Y-%m-%d")

FILENAMES = [
    f"mlb_rosters_{DATE}.json",
    f"mlb_probable_starters_{DATE}.json",
    f"mlb_boxscores_{DATE}.json",
    f"mlb_weather_{DATE}.json",
    f"mlb_betting_odds_{DATE}.json",
    f"mlb_espn_articles_{DATE}.json",
    f"reddit_fantasybaseball_articles_{DATE}.json"
]

os.makedirs("mlb_json_inputs", exist_ok=True)

for fname in FILENAMES:
    key = f"mlb/json/{fname}"
    local_path = os.path.join("mlb_json_inputs", fname)
    try:
        s3.download_file("fantasy-sports-csvs", key, local_path)
        print(f"✅ Downloaded: {key}")
    except Exception as e:
        print(f"❌ Failed to download {key}: {e}")
