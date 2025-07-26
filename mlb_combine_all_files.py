#!/usr/bin/env python3
import os
import re
import json
import sys
import boto3
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# ───── DATE CONFIG ─────
DATE = os.getenv("FORCE_DATE", datetime.now().strftime("%Y-%m-%d"))
YDAY = (datetime.strptime(DATE, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

BASE = "baseball"  # folder prefix preserved by S3 sync

# ───── FILE PATHS ─────
FILE_ROSTER   = f"{BASE}/rosters/mlb_rosters_{DATE}.json"
FILE_STARTERS = f"{BASE}/probablestarters/mlb_probable_starters_{DATE}.json"
FILE_WEATHER  = f"{BASE}/weather/mlb_weather_{DATE}.json"
FILE_ODDS     = f"{BASE}/betting/mlb_betting_odds_{DATE}.json"
FILE_ESPN     = f"{BASE}/news/mlb_espn_articles_{DATE}.json"
FILE_REDDIT   = f"{BASE}/news/reddit_fantasybaseball_articles_{DATE}.json"
FILE_BOX      = f"{BASE}/boxscores/mlb_boxscores_{YDAY}.json"

OUT_FILE = f"structured_players_{DATE}.json"

UPLOAD_TO_S3 = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"
BUCKET       = "fantasy-sports-csvs"
S3_KEY       = f"{BASE}/combined/{OUT_FILE}"
REGION       = "us-east-1"

def load_json(path):
    if not os.path.exists(path):
        print(f"⚠️  {path} not found — skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize(name: str) -> str:
    return re.sub(r"[ .'-]", "", name).lower()

# ───── LOAD INPUT FILES ─────
rosters   = load_json(FILE_ROSTER)
starters  = load_json(FILE_STARTERS)
weather   = load_json(FILE_WEATHER)
odds      = load_json(FILE_ODDS)
espn      = load_json(FILE_ESPN)
reddit    = load_json(FILE_REDDIT)
boxscores = load_json(FILE_BOX)

if not rosters:
    sys.exit("❌ Roster file missing — cannot build structured players.")

# ───── INDEX DATA ─────
weather_by_team = {w["team"]: w for w in weather}
box_by_pid = {str(b.get("player_id") or b.get("id") or b.get("mlb_id")): b for b in boxscores}
starter_names = {normalize(g.get("home_pitcher", "")) for g in starters} |                 {normalize(g.get("away_pitcher", "")) for g in starters}

# Team to opponent mapping (by team name)
team_to_gamepk = {}
team_to_opp = {}

for g in starters:
    home = g.get("home_team_name")
    away = g.get("away_team_name")
    pk   = g.get("game_pk")
    if home and away and pk:
        team_to_gamepk[home] = pk
        team_to_gamepk[away] = pk
        team_to_opp[home] = away
        team_to_opp[away] = home

# Betting odds mapping by team name
bet_by_team = {}
for o in odds:
    tname = o.get("team_name") or o.get("team") or o.get("team_id")
    if tname:
        bet_by_team[str(tname)] = o

# Mentions
espn_cnt = Counter()
reddit_cnt = Counter()
for art in espn:
    title = str(art.get("headline", "")).lower()
    for r in rosters:
        if r["player"].split()[-1].lower() in title:
            espn_cnt[r["player_id"]] += 1
for post in reddit:
    txt = str(post.get("title", "")).lower()
    for r in rosters:
        if r["player"].split()[-1].lower() in txt:
            reddit_cnt[r["player_id"]] += 1

# ───── BUILD STRUCTURED PLAYERS ─────
players_out = {}

for r in rosters:
    pid = str(r["player_id"])
    name = r["player"]
    team = r["team"]
    team_name = str(r.get("team_id", team))
    pos = r["position"]

    players_out[name] = {
        "player_id": pid,
        "name": name,
        "team": team,
        "team_id": team_name,
        "position": pos,
        "handedness": {
            "bats": r.get("bats", ""),
            "throws": r.get("throws", "")
        },
        "roster_status": {
            "status_code": r.get("status_code", ""),
            "status_description": r.get("status_description", "")
        },
        "starter": normalize(name) in starter_names if pos == "P" else False,
        "opponent_team_id": team_to_opp.get(team),
        "game_pk": team_to_gamepk.get(team),
        "weather_context": weather_by_team.get(team),
        "betting_context": bet_by_team.get(team),
        "espn_mentions": espn_cnt.get(r["player_id"], 0),
        "reddit_mentions": reddit_cnt.get(r["player_id"], 0),
        "box_score": box_by_pid.get(pid, {})
    }

print(f"✅ Built structured entries for {len(players_out)} players.")

# ───── SAVE TO DISK ─────
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_out, f, ensure_ascii=False, indent=2)
print(f"💾 Wrote {OUT_FILE}")

# ───── S3 UPLOAD ─────
if UPLOAD_TO_S3:
    try:
        boto3.client("s3", region_name=REGION).upload_file(OUT_FILE, BUCKET, S3_KEY)
        print(f"☁️  Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
