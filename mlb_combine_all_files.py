import json
import os
from datetime import datetime

DATE = datetime.now().strftime("%Y-%m-%d")
INPUT_DIR = "./mlb_json_inputs"
OUTPUT_FILE = f"mlb_structured_players_{DATE}.json"
OUTPUT_PATH = os.path.join(".", OUTPUT_FILE)

# Load all JSON files
def load_json(name):
    path = os.path.join(INPUT_DIR, name)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

rosters = load_json(f"mlb_rosters_{DATE}.json")
probable_starters = load_json(f"mlb_probable_starters_{DATE}.json")
boxscores = load_json(f"mlb_boxscores_{DATE}.json")
weather = load_json(f"mlb_weather_{DATE}.json")
odds = load_json(f"mlb_betting_odds_{DATE}.json")
espn = load_json(f"mlb_espn_articles_{DATE}.json")
reddit = load_json(f"reddit_fantasybaseball_articles_{DATE}.json")

# Extract probable starter IDs
starter_ids = {p['player_id'] for p in probable_starters if 'player_id' in p}

# Build player data
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
            "bats": p.get("bat_side", "R"),   # Default to R if not provided
            "throws": p.get("throw_side", "R")
        },
        "box_score": {},
        "betting_context": {},
        "weather_context": {},
        "espn_mentions": [],
        "reddit_mentions": []
    }

# Attach box score stats
for row in boxscores:
    name = row.get("player", "")
    if name in players:
        players[name]["box_score"] = row

# Optionally attach weather context by team
for w in weather:
    team = w.get("team", "")
    for p in players.values():
        if p["team"] == team:
            p["weather_context"] = w

# Optionally attach betting odds by team
for o in odds:
    team = o.get("team", "")
    for p in players.values():
        if p["team"] == team:
            p["betting_context"] = o

# ESPN mentions (matched by partial player name)
for art in espn:
    for name in players:
        if name in art.get("content", ""):
            players[name]["espn_mentions"].append(art)

# Reddit mentions (matched similarly)
for post in reddit:
    for name in players:
        if name in post.get("content", ""):
            players[name]["reddit_mentions"].append(post)

# Output
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(players, f, indent=2, ensure_ascii=False)

print(f"✅ Player-level JSON written: {OUTPUT_PATH}")
