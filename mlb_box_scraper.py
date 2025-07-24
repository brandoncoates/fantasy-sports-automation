import os
import requests
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import boto3

from shared.normalize_name import normalize_name

# === CONFIG ===
# Compute "yesterday" in Eastern Time so it matches the MLB API calendar
eastern_now = datetime.now(ZoneInfo("America/New_York"))
target_date = (eastern_now - timedelta(days=1)).strftime("%Y-%m-%d")

BUCKET    = "fantasy-sports-csvs"
S3_FOLDER = "baseball/boxscores"
filename  = f"mlb_boxscores_{target_date}.json"
s3_key    = f"{S3_FOLDER}/{filename}"

# Helpers to coerce stats into numbers
def to_int(val):
    try:
        return int(val)
    except:
        return 0

def to_float(val):
    try:
        return float(val)
    except:
        return 0.0

# === STEP 1: Fetch schedule for target_date ===
schedule_url = (
    f"https://statsapi.mlb.com/api/v1/schedule"
    f"?sportId=1&date={target_date}"
)
resp   = requests.get(schedule_url)
resp.raise_for_status()
dates  = resp.json().get("dates", [])
games  = dates[0].get("games", []) if dates else []
game_ids = [g["gamePk"] for g in games]

print(f"Found {len(game_ids)} games for {target_date}")

# === STEP 2: Pull boxscores & normalize names ===
records = []
for game_id in game_ids:
    try:
        box = requests.get(
            f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
        ).json()
        for side in ("home", "away"):
            team_name = box["teams"][side]["team"]["name"]
            players   = box["teams"][side]["players"].values()
            for player in players:
                raw_name = player["person"].get("fullName", "")
                name     = normalize_name(raw_name)

                bat = player.get("stats", {}).get("batting", {})
                pit = player.get("stats", {}).get("pitching", {})

                ip  = to_float(pit.get("inningsPitched"))
                er  = to_int(pit.get("earnedRuns"))
                qs  = 1 if (ip >= 6 and er <= 3) else 0

                records.append({
                    "Game Date":              target_date,
                    "Game ID":                game_id,
                    "Team":                   team_name,
                    "Player Name":            name,
                    "Position":               ", ".join(
                        pos["abbreviation"] for pos in player.get("allPositions", [])
                    ),

                    # Batting stats
                    "At Bats":                to_int(bat.get("atBats")),
                    "Runs":                   to_int(bat.get("runs")),
                    "Hits":                   to_int(bat.get("hits")),
                    "Doubles":                to_int(bat.get("doubles")),
                    "Triples":                to_int(bat.get("triples")),
                    "Home Runs":              to_int(bat.get("homeRuns")),
                    "RBIs":                   to_int(bat.get("rbi")),
                    "Walks":                  to_int(bat.get("baseOnBalls")),
                    "Strikeouts (Bat)":       to_int(bat.get("strikeOuts")),
                    "Stolen Bases":           to_int(bat.get("stolenBases")),

                    # Pitching stats
                    "Innings Pitched":        ip,
                    "Earned Runs":            er,
                    "Strikeouts (Pitching)":  to_int(pit.get("strikeOuts")),
                    "Wins":                   to_int(pit.get("wins")),
                    "Quality Start":          qs
                })
    except Exception as e:
        print(f"❌ Skipped game {game_id} due to error: {e}")

# === STEP 3: Write JSON locally ===
os.makedirs("mlb_box_scores", exist_ok=True)
local_path = os.path.join("mlb_box_scores", filename)
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)
print(f"💾 JSON written locally: {local_path}")

# === STEP 4: Upload JSON to S3 ===
s3 = boto3.client("s3")
try:
    s3.upload_file(local_path, BUCKET, s3_key)
    print(f"☁️ Uploaded to s3://{BUCKET}/{s3_key}")
except Exception as e:
    print(f"❌ S3 upload failed: {e}")
    exit(1)
