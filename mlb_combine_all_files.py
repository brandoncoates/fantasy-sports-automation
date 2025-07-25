import json
import os
from datetime import datetime
import boto3

# === CONFIG ===
DATE = datetime.now().strftime("%Y-%m-%d")
INPUT_DIR = "./mlb_json_inputs"
OUTPUT_FILE = f"mlb_structured_players_{DATE}.json"
OUTPUT_PATH = os.path.join(".", OUTPUT_FILE)

REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/combined"
S3_KEY = f"{S3_FOLDER}/{OUTPUT_FILE}"

# === Load JSON Utility ===
def load_json(name):
    path = os.path.join(INPUT_DIR, name)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

# === Load All Files ===
rosters = load_json(f"mlb_rosters_{DATE}.json")
probable_starters = load_json(f"mlb_probable_starters_{DATE}.json")
boxscores = load_json(f"mlb_boxscores_{DATE}.json")
weather = load_json(f"mlb_weather_{DATE}.json")
odds = load_json(f"mlb_betting_odds_{DATE}.json")
espn = load_json(f"mlb_espn_articles_{DATE}.json")
reddit = load_json(f"reddit_fantasybaseball_articles_{DATE}.json")

# === Identify Probable Starters ===
starter_ids = {str(p['player_id']) for p in probable_starters if 'player_id' in p}

# === Build Player Records ===
players = {}

for p in rosters:
    name = p["player"]
    pid = str(p["player_id"])
    players[name] = {
        "player_id": pid,
        "team": p.get("team"),
        "position": p.get("position"),
        "starter": pid in starter_ids,
        "roster_status": {
            "status_code": p.get("status_code"),
            "status_description": p.get("status_description")
        },
        "handedness": {
            "bats": p.get("bat_side", "R"),
            "throws": p.get("throw_side", "R")
        },
        "box_score": {},
        "betting_context": {},
        "weather_context": {},
        "espn_mentions": [],
        "reddit_mentions": []
    }

# === Attach Box Score Stats ===
for row in boxscores:
    name = row.get("player", "")
    if name in players:
        players[name]["box_score"] = row

# === Attach Weather Data by Team ===
for w in weather:
    team = w.get("team", "")
    for p in players.values():
        if p["team"] == team:
            p["weather_context"] = w

# === Attach Betting Odds by Team ===
for o in odds:
    team = o.get("team", "")
    for p in players.values():
        if p["team"] == team:
            p["betting_context"] = o

# === ESPN Mentions ===
for art in espn:
    for name in players:
        if name in art.get("content", ""):
            players[name]["espn_mentions"].append(art)

# === Reddit Mentions ===
for post in reddit:
    for name in players:
        if name in post.get("content", ""):
            players[name]["reddit_mentions"].append(post)

# === Write JSON File ===
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(players, f, indent=2, ensure_ascii=False)

print(f"✅ Player-level JSON written: {OUTPUT_PATH}")

# === Upload to S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)

try:
    s3.upload_file(OUTPUT_PATH, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
