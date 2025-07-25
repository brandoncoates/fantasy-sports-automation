import boto3
import os
from datetime import datetime

# === CONFIG ===
s3 = boto3.client("s3")
BUCKET = "fantasy-sports-csvs"
DATE = datetime.now().strftime("%Y-%m-%d")
OUTPUT_DIR = "mlb_json_inputs"

# === MAP FILENAMES TO ACTUAL FOLDERS (with underscores) ===
files = {
    "mlb_rosters": f"rosters/mlb_rosters_{DATE}.json",
    "mlb_probable_starters": f"probable_starters/mlb_probable_starters_{DATE}.json",
    "mlb_boxscores": f"boxscores/mlb_boxscores_{DATE}.json",
    "mlb_weather": f"weather/mlb_weather_{DATE}.json",
    "mlb_betting_odds": f"betting_odds/mlb_betting_odds_{DATE}.json",
    "mlb_espn_articles": f"news/mlb_espn_articles_{DATE}.json",
    "reddit_fantasybaseball_articles": f"news/reddit_fantasybaseball_articles_{DATE}.json",
}

# === Make Output Directory ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Download Each File ===
for label, key in files.items():
    local_path = os.path.join(OUTPUT_DIR, os.path.basename(key))
    s3_key = f"baseball/{key}"
    try:
        s3.download_file(BUCKET, s3_key, local_path)
        print(f"✅ Downloaded {s3_key} → {local_path}")
    except Exception as e:
        print(f"❌ Failed to download {s3_key}: {e}")
