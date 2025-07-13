import os
import csv
import requests
from datetime import datetime
import boto3

# === CONFIG ===
REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/probablestarters"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_probable_starters_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === BUILD URL ===
url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={DATE}&hydrate=probablePitcher(note,stats,person)"

print(f"📡 Requesting probable starters from MLB API for {DATE}")
response = requests.get(url)
if response.status_code != 200:
    print(f"❌ API error {response.status_code}: {response.text}")
    exit(1)

data = response.json()
games = data.get("dates", [])[0].get("games", []) if data.get("dates") else []

rows = []
for game in games:
    game_id = game.get("gamePk")
    home = game.get("teams", {}).get("home", {})
    away = game.get("teams", {}).get("away", {})

    home_team = home.get("team", {}).get("name", "")
    away_team = away.get("team", {}).get("name", "")

    home_pitcher_data = home.get("probablePitcher", {})
    away_pitcher_data = away.get("probablePitcher", {})

    home_pitcher = home_pitcher_data.get("fullName", "")
    away_pitcher = away_pitcher_data.get("fullName", "")

    home_throw_hand = home_pitcher_data.get("pitchHand", {}).get("description", "")
    away_throw_hand = away_pitcher_data.get("pitchHand", {}).get("description", "")

    rows.append({
        "date": DATE,
        "game_id": game_id,
        "away_team": away_team,
        "away_pitcher": away_pitcher,
        "away_throw_hand": away_throw_hand,
        "home_team": home_team,
        "home_pitcher": home_pitcher,
        "home_throw_hand": home_throw_hand
    })

print(f"✅ Found {len(rows)} games with probable pitchers")

# === SAVE TEMP FILE ===
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
