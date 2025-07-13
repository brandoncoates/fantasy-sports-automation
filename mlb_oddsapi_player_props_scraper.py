import os
import csv
import boto3
import requests
from datetime import datetime

# === CONFIG ===
API_KEY = os.environ.get("ODDS_API_KEY")
REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/playerprops"
SPORT = "baseball_mlb"
REGIONS = "us"
MARKETS = "player_props"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_oddsapi_player_props_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === BUILD URL ===
url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
params = {
    "apiKey": API_KEY,
    "regions": REGIONS,
    "markets": MARKETS,
    "oddsFormat": "decimal"
}

print(f"📡 Requesting player props from Odds API...")
response = requests.get(url, params=params)
if response.status_code != 200:
    print(f"❌ API error {response.status_code}: {response.text}")
    exit(1)

data = response.json()
print(f"📊 Received {len(data)} player prop events")

# === PROCESS DATA ===
rows = []
for event in data:
    game = event.get("home_team") + " vs " + event.get("away_team")
    commence_time = event.get("commence_time")
    for bookmaker in event.get("bookmakers", []):
        book_name = bookmaker.get("title")
        for market in bookmaker.get("markets", []):
            market_key = market.get("key")
            for outcome in market.get("outcomes", []):
                rows.append({
                    "date": DATE,
                    "game": game,
                    "player": outcome.get("name"),
                    "prop": market_key,
                    "book": book_name,
                    "value": outcome.get("point"),
                    "odds": outcome.get("price"),
                    "time": commence_time
                })

print(f"✅ Prepared {len(rows)} prop rows for upload")

# === SAVE LOCALLY (TEMPORARY FOR DEBUGGING) ===
csv_file = FILENAME
with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"💾 Temp file written locally: {csv_file}")

# === UPLOAD TO S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(csv_file, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === CLEANUP TEMP FILE ===
os.remove(csv_file)
print(f"🧹 Cleaned up local file {csv_file}")
