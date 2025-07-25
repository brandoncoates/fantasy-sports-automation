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
            data = json.load(f)
            print(f"📄 Loaded {name} with {len(data)} entries")
            return data
    print(f"⚠️ File not found: {name}")
    return []

# === Load All Files ===
rosters           = load_json(f"mlb_rosters_{DATE}.json")
probable_starters = load_json(f"mlb_probable_starters_{DATE}.json")
boxscores         = load_json(f"mlb_boxscores_{DATE}.json")
weather           = load_json(f"mlb_weather_{DATE}.json")
odds              = load_json(f"mlb_betting_odds_{DATE}.json")
espn              = load_json(f"mlb_espn_articles_{DATE}.json")
reddit            = load_json(f"reddit_fantasybaseball_articles_{DATE}.json")

# === Index Probable Starters by Name ===
starter_names = set()
for game in probable_starters:
    away = game.get("away_pitcher")
    home = game.get("home_pitcher")
    if away:
        starter_names.add(away)
    if home:
        starter_names.add(home)

# === Build Player Records ===
players = {}
for p in rosters:
    name = p.get("player", "").strip()
    if not name:
        continue
    pid = str(p.get("player_id", ""))
    players[name] = {
        "player_id": pid,
        "team": p.get("team", ""),
        "position": p.get("position", ""),
        "starter": name in starter_names,
        "roster_status": {
            "status_code": p.get("status_code"),
            "status_description": p.get("status_description")
        },
        "handedness": {
            "bats": p.get("bat_side", "R"),
            "throws": p.get("throw_side", "R")
        },
        "box_score": {},
        "weather_context": {},
        "betting_context": {},
        "espn_mentions": [],
        "reddit_mentions": []
    }

# === Attach Box Score Stats ===
for row in boxscores:
    pname = row.get("player", "")
    if pname in players:
        players[pname]["box_score"] = row

# === Attach Weather by Team ===
for w in weather:
    team = w.get("team", "")
    for rec in players.values():
        if rec["team"] == team:
            rec["weather_context"] = w

# === Attach Betting Odds by Team ===
for o in odds:
    team = o.get("team", "")
    for rec in players.values():
        if rec["team"] == team:
            rec["betting_context"] = o

# === Attach ESPN Mentions ===
for art in espn:
    content = art.get("content", "")
    for name, rec in players.items():
        if name in content:
            rec["espn_mentions"].append(art)

# === Attach Reddit Mentions ===
for post in reddit:
    content = post.get("content", "")
    for name, rec in players.items():
        if name in content:
            rec["reddit_mentions"].append(post)

# === Write & Upload ===
if players:
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(players, f, indent=2, ensure_ascii=False)
    print(f"✅ Player JSON written: {OUTPUT_PATH}")

    try:
        s3 = boto3.client("s3", region_name=REGION)
        s3.upload_file(OUTPUT_PATH, BUCKET, S3_KEY)
        print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
else:
    print("⚠️ No players found, skipping upload.")
