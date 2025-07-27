#!/usr/bin/env python3
import os
import re
import json
import sys
import boto3
import pytz
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# ───── TIMEZONE-AWARE DATE ─────
pst = pytz.timezone("US/Pacific")
DATE = os.getenv("FORCE_DATE", datetime.now(pst).strftime("%Y-%m-%d"))
YDAY = (datetime.strptime(DATE, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

# ───── CONFIG ─────
BASE     = "baseball"
OUT_FILE = f"structured_players_{DATE}.json"
UPLOAD   = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"
BUCKET   = "fantasy-sports-csvs"
REGION   = "us-east-1"
S3_KEY   = f"{BASE}/combined/{OUT_FILE}"

# ───── PATHS ─────
paths = {
    "roster":   f"{BASE}/rosters/mlb_rosters_{DATE}.json",
    "starters": f"{BASE}/probablestarters/mlb_probable_starters_{DATE}.json",
    "weather":  f"{BASE}/weather/mlb_weather_{DATE}.json",
    "odds":     f"{BASE}/betting/mlb_betting_odds_{DATE}.json",
    "espn":     f"{BASE}/news/mlb_espn_articles_{DATE}.json",
    "reddit":   f"{BASE}/news/reddit_fantasybaseball_articles_{DATE}.json",
    "box":      f"{BASE}/boxscores/mlb_boxscores_{YDAY}.json",
}

def load_json(path):
    if not os.path.exists(path):
        print(f"⚠️ {path} not found — skipping.")
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def normalize(name):
    return re.sub(r"[ .'-]", "", name).lower()

# ───── TEAM NAME MAPPING ─────
TEAM_NAME_MAP = {
    "Giants": "San Francisco Giants",
    "Dodgers": "Los Angeles Dodgers",
    "Yankees": "New York Yankees",
    "Mets": "New York Mets",
    "Red Sox": "Boston Red Sox",
    "White Sox": "Chicago White Sox",
    "Cubs": "Chicago Cubs",
    # Add more mappings as needed
}

def normalize_team_name(name):
    return TEAM_NAME_MAP.get(name, name)

# ───── LOAD DATA ─────
rosters   = load_json(paths["roster"])
starters  = load_json(paths["starters"])
weather   = load_json(paths["weather"])
odds      = load_json(paths["odds"])
espn      = load_json(paths["espn"])
reddit    = load_json(paths["reddit"])
boxscores = load_json(paths["box"])

if not rosters:
    sys.exit("❌ Roster file missing — cannot proceed.")

# ───── INDEXES ─────
starter_names = {normalize(g.get("home_pitcher", "")) for g in starters} | {normalize(g.get("away_pitcher", "")) for g in starters}

weather_by_team = {normalize_team_name(w.get("team", "")): w for w in weather}
betting_by_team = {normalize_team_name(o.get("team", "")): o for o in odds}
box_by_pid = {str(b.get("player_id") or b.get("id") or b.get("mlb_id")): b for b in boxscores}

# Article mentions
espn_mentions = Counter()
reddit_mentions = Counter()
for article in espn:
    title = article.get("headline", "").lower()
    for r in rosters:
        if r.get("last_name", "").lower() in title:
            espn_mentions[r["player_id"]] += 1

for post in reddit:
    title = post.get("title", "").lower()
    for r in rosters:
        if r.get("last_name", "").lower() in title:
            reddit_mentions[r["player_id"]] += 1

# Game context
team_to_gamepk = {}
team_to_opp = {}
for g in starters:
    home = g.get("home_team_id")
    away = g.get("away_team_id")
    pk = g.get("game_pk")
    if home and away:
        team_to_gamepk[home] = team_to_gamepk[away] = pk
        team_to_opp[home] = away
        team_to_opp[away] = home

# ───── BUILD OUTPUT ─────
players_out = {}

for r in rosters:
    pid = str(r.get("player_id", ""))
    if not pid:
        continue

    name = f'{r.get("first_name", "")} {r.get("last_name", "")}'.strip()
    raw_team = r.get("team", "")
    team = normalize_team_name(raw_team)
    tid = r.get("team_id")

    players_out[name] = {
        "player_id": pid,
        "name": name,
        "team": team,
        "team_id": tid,
        "position": r.get("position"),
        "handedness": {
            "bats": r.get("bats"),
            "throws": r.get("throws")
        },
        "roster_status": {
            "status_code": r.get("status_code"),
            "status_description": r.get("status_description"),
        },
        "starter": normalize(name) in starter_names if r.get("position") == "P" else False,
        "opponent_team_id": team_to_opp.get(tid),
        "game_pk": team_to_gamepk.get(tid),
        "weather_context": weather_by_team.get(team),
        "betting_context": betting_by_team.get(team),
        "espn_mentions": espn_mentions.get(r["player_id"], 0),
        "reddit_mentions": reddit_mentions.get(r["player_id"], 0),
        "box_score": box_by_pid.get(pid, {})
    }

print(f"✅ Structured data for {len(players_out)} players.")

# ───── WRITE FILE ─────
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_out, f, ensure_ascii=False, indent=2)
print(f"💾 Wrote file: {OUT_FILE}")

# ───── UPLOAD TO S3 ─────
if UPLOAD:
    try:
        s3 = boto3.client("s3", region_name=REGION)
        s3.upload_file(OUT_FILE, BUCKET, S3_KEY)
        print(f"☁️ Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
