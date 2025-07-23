import os
import requests
import json
from datetime import datetime, timedelta
import boto3

# import the normalization function
from shared.normalize_name import normalize_name

# === CONFIG: Set True to pull yesterday's games ===
use_yesterday = True

# Get date in YYYY-MM-DD format
if use_yesterday:
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
else:
    target_date = datetime.now().strftime('%Y-%m-%d')

# AWS / S3 config
BUCKET    = "fantasy-sports-csvs"
S3_FOLDER = "baseball/boxscores"
filename  = f"mlb_boxscores_{target_date}.json"
s3_key    = f"{S3_FOLDER}/{filename}"

# Step 1: Fetch today's schedule
schedule_url = f'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={target_date}'
resp         = requests.get(schedule_url)
games        = (resp.json().get('dates') or [{}])[0].get('games', [])
game_ids     = [g['gamePk'] for g in games]

print(f"Found {len(game_ids)} games for {target_date}.")

# Step 2: Pull boxscores and normalize names
records = []
for game_id in game_ids:
    try:
        box = requests.get(f'https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore').json()
        for side in ("home", "away"):
            team_name = box["teams"][side]["team"]["name"]
            for player in box["teams"][side]["players"].values():
                person   = player.get("person", {})
                raw_name = person.get("fullName", "")
                name     = normalize_name(raw_name)
                stats    = player.get("stats", {})
                bat      = stats.get("batting", {})
                pit      = stats.get("pitching", {})

                records.append({
                    "Game Date":       target_date,
                    "Game ID":         game_id,
                    "Team":            team_name,
                    "Player Name":     name,
                    "Position":        ", ".join(p["abbreviation"] for p in player.get("allPositions", [])),

                    # batting
                    "At Bats":           bat.get("atBats"),
                    "Runs":              bat.get("runs"),
                    "Hits":              bat.get("hits"),
                    "Doubles":           bat.get("doubles"),
                    "Triples":           bat.get("triples"),
                    "Home Runs":         bat.get("homeRuns"),
                    "RBIs":              bat.get("rbi"),
                    "Walks":             bat.get("baseOnBalls"),
                    "Strikeouts (Bat)":  bat.get("strikeOuts"),
                    "Stolen Bases":      bat.get("stolenBases"),

                    # pitching
                    "Innings Pitched":        pit.get("inningsPitched"),
                    "Earned Runs":            pit.get("earnedRuns"),
                    "Strikeouts (Pitching)":  pit.get("strikeOuts"),
                    "Wins":                   pit.get("wins"),
                    "Quality Start":          int(pit.get("inningsPitched", 0) >= 6 and pit.get("earnedRuns", 0) <= 3),
                })
    except Exception as e:
        print(f"❌ Skipped game {game_id}: {e}")

# Step 3: Save JSON locally
os.makedirs("mlb_box_scores", exist_ok=True)
local_path = os.path.join("mlb_box_scores", filename)
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)
print(f"💾 JSON written locally: {local_path}")

# Step 4: Upload JSON to S3
s3 = boto3.client("s3")
try:
    s3.upload_file(local_path, BUCKET, s3_key)
    print(f"☁️ Uploaded to s3://{BUCKET}/{s3_key}")
except Exception as e:
    print(f"❌ S3 upload failed: {e}")
    exit(1)
