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
rosters = load_json(f"mlb_rosters_{DATE}.json")
probable_starters = load_json(f"mlb_probable_starters_{DATE}.json")
boxscores = load_json(f"mlb_boxscores_{DATE}.json")
weather = load_json(f"mlb_weather_{DATE}.json")
odds = load_json(f"mlb_betting_odds_{DATE}.json")
espn = load_json(f"mlb_espn_articles_{DATE}.json")
reddit = load_json(f"reddit_fantasybaseball_articles_{DATE}.json")

# === Identify Probable Starters by **Name** ===
starter_names = set()
for ps in probable_starters:
    # add both away and home pitcher names if present
    if ps.get("away_pitcher"):
        starter_names.add(ps["away_pitcher"])
    if ps.get("home_pitcher"):
        starter_names.add(ps["home_pitcher"])

# === Build Player Records ===
players = {}

for p in rosters:
    name = p.get("player", "").strip()
    if not name:
        continue
    pid = str(p.get("player_id", ""))
    players[name] = {
        "player_id": pid,
        "team": p.get("team"),
        "position": p.get("position"),
        # **Now match on the player's name against the starter_names set**
        "starter": name in starter_names,
        "roster_status": {
            "status_code": p.get("status_code"),
            "status_description": p.get("status_description")
        },
        "handedness": {
            "bats": p.get("bats", p.get("bat_side", "R")),
            "throws": p.get("throws", p.get("throw_side", "R"))
        },
        "box_score": {},
        "betting_context": {},
        "weather_context": {},
        "espn_mentions": [],
        "reddit_mentions": []
    }

# === Attach Box Score Stats ===
for row in boxscores:
    pname = row.get("player", "")
    if pname in players:
        players[pname]["box_score"] = row

# === Attach Weather Data by Team ===
for w in weather:
    team = w.get("team", "")
    for p in players.values():
        if p["team"] == team:
