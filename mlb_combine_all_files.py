#!/usr/bin/env python3
import os
import re
import json
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import pytz

# ───── TIMEZONE-SAFE DATE ─────
pst = pytz.timezone("US/Pacific")
DATE = os.getenv("FORCE_DATE", datetime.now(pst).strftime("%Y-%m-%d"))
YDAY = (datetime.strptime(DATE, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

# ───── PATH CONFIG ─────
BASE = "baseball"
ROSTER   = f"{BASE}/rosters/mlb_rosters_{DATE}.json"
STARTERS = f"{BASE}/probablestarters/mlb_probable_starters_{DATE}.json"
WEATHER  = f"{BASE}/weather/mlb_weather_{DATE}.json"
ODDS     = f"{BASE}/betting/mlb_betting_odds_{DATE}.json"
ESPN     = f"{BASE}/news/mlb_espn_articles_{DATE}.json"
REDDIT   = f"{BASE}/news/reddit_fantasybaseball_articles_{DATE}.json"
BOX      = f"{BASE}/boxscores/mlb_boxscores_{YDAY}.json"

OUT_FILE = f"structured_players_{DATE}.json"

# ───── S3 CONFIG (OPTIONAL) ─────
UPLOAD_TO_S3 = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"
BUCKET       = "fantasy-sports-csvs"
S3_KEY       = f"{BASE}/combined/{OUT_FILE}"
REGION       = "us-east-1"

# ───── HELPERS ─────
def load_json(path):
    if not os.path.exists(path):
        print(f"⚠️ {path} not found — skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize(text):
    return re.sub(r"[ .'-]", "", text).lower()

TEAM_NAME_MAP = {
    "giants": "San Francisco Giants",
    "dodgers": "Los Angeles Dodgers",
    "yankees": "New York Yankees",
    "mets": "New York Mets",
    "redsox": "Boston Red Sox",
    "whitesox": "Chicago White Sox",
    "cubs": "Chicago Cubs",
    "cardinals": "St. Louis Cardinals",
    "padres": "San Diego Padres",
    # Add more as needed
}

# ───── LOAD DATA FILES ─────
rosters   = load_json(ROSTER)
starters  = load_json(STARTERS)
weather   = load_json(WEATHER)
odds      = load_json(ODDS)
espn      = load_json(ESPN)
reddit    = load_json(REDDIT)
boxscores = load_json(BOX)

if not rosters:
    raise SystemExit("❌ No roster data — cannot proceed.")

# ───── BUILD INDEXES ─────
weather_by_team = {
    TEAM_NAME_MAP.get(normalize(w["team"]), w["team"]): w for w in weather if "team" in w
}
box_by_pid = {
    str(b.get("player_id") or b.get("id") or b.get("mlb_id")): b for b in boxscores
}
starter_names = {
    normalize(g.get("home_pitcher", "")) for g in starters
} | {
    normalize(g.get("away_pitcher", "")) for g in starters
}
bet_by_team = {}
for o in odds:
    team = o.get("team") or o.get("team_name") or ""
    team_norm = TEAM_NAME_MAP.get(normalize(team), team)
    bet_by_team[team_norm] = o

# ───── COUNT MENTIONS ─────
espn_cnt, reddit_cnt = Counter(), Counter()
for article in espn:
    title = article.get("headline", "").lower()
    for r in rosters:
        if normalize(r["player"].split()[-1]) in normalize(title):
            espn_cnt[r["player_id"]] += 1

for post in reddit:
    title = post.get("title", "").lower()
    for r in rosters:
        if normalize(r["player"].split()[-1]) in normalize(title):
            reddit_cnt[r["player_id"]] += 1

# ───── BUILD STRUCTURED OUTPUT ─────
players_out = {}

for r in rosters:
    pid = str(r["player_id"])
    name = r["player"].strip()
    team = r.get("team")
    position = r.get("position", "")
    team_norm = TEAM_NAME_MAP.get(normalize(team), team)

    players_out[name] = {
        "player_id": pid,
        "name": name,
        "team": team,
        "position": position,
        "handedness": {
            "bats": r.get("bats"),
            "throws": r.get("throws"),
        },
        "roster_status": {
            "status_code": r.get("status_code"),
            "status_description": r.get("status_description"),
        },
        "starter": normalize(name) in starter_names if position == "P" else False,
        "weather_context": weather_by_team.get(team_norm, {}),
        "betting_context": bet_by_team.get(team_norm, {}),
        "espn_mentions": espn_cnt.get(r["player_id"], 0),
        "reddit_mentions": reddit_cnt.get(r["player_id"], 0),
        "box_score": box_by_pid.get(pid, {}),
    }

# ───── SAVE FILE ─────
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_out, f, indent=2)
print(f"✅ Wrote {len(players_out)} player entries to {OUT_FILE}")

# ───── OPTIONAL UPLOAD ─────
if UPLOAD_TO_S3:
    import boto3
    try:
        s3 = boto3.client("s3", region_name=REGION)
        s3.upload_file(OUT_FILE, BUCKET, S3_KEY)
        print(f"☁️ Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
