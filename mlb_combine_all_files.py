#!/usr/bin/env python3
"""
mlb_combine_all_files.py
────────────────────────
Create today's master structured_players_{DATE}.json by merging:

    baseball/rosters/mlb_rosters_{DATE}.json              (REQUIRED)
    baseball/probablestarters/mlb_probable_starters_{DATE}.json
    baseball/weather/mlb_weather_{DATE}.json
    baseball/betting/mlb_betting_odds_{DATE}.json
    baseball/news/mlb_espn_articles_{DATE}.json
    baseball/news/reddit_fantasybaseball_articles_{DATE}.json
    baseball/boxscores/mlb_boxscores_{YESTERDAY}.json

Output is written to repo root and (optionally) uploaded to:
    baseball/combined/structured_players_{DATE}.json
"""

import os, re, json, sys, boto3
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# ─────────── CONFIG ───────────
DATE = datetime.now().strftime("%Y-%m-%d")                # e.g., 2025‑07‑26
YDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

BASE = "baseball"                                         # top‑level folder

# File paths
FILE_ROSTER   = f"{BASE}/rosters/mlb_rosters_{DATE}.json"
FILE_STARTERS = f"{BASE}/probablestarters/mlb_probable_starters_{DATE}.json"
FILE_WEATHER  = f"{BASE}/weather/mlb_weather_{DATE}.json"
FILE_ODDS     = f"{BASE}/betting/mlb_betting_odds_{DATE}.json"
FILE_ESPN     = f"{BASE}/news/mlb_espn_articles_{DATE}.json"
FILE_REDDIT   = f"{BASE}/news/reddit_fantasybaseball_articles_{DATE}.json"
FILE_BOX      = f"{BASE}/boxscores/mlb_boxscores_{YDAY}.json"

OUT_FILE      = f"structured_players_{DATE}.json"

# Optional S3 push
UPLOAD_TO_S3  = False
BUCKET        = "fantasy-sports-csvs"
S3_KEY        = f"{BASE}/combined/{OUT_FILE}"
REGION        = "us-east-1"

# ─────────── HELPERS ───────────
def load_json(path):
    """Return list[dict] or dict loaded from JSON; [] if missing."""
    if not os.path.exists(path):
        print(f"⚠️  {path} not found — skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize(name: str) -> str:
    return re.sub(r"[ .'-]", "", name).lower()

# ─────────── LOAD FEEDS ───────────
rosters   = load_json(FILE_ROSTER)        # REQUIRED
starters  = load_json(FILE_STARTERS)
weather   = load_json(FILE_WEATHER)
odds      = load_json(FILE_ODDS)
espn      = load_json(FILE_ESPN)
reddit    = load_json(FILE_REDDIT)
boxscores = load_json(FILE_BOX)

if not rosters:
    sys.exit("❌ Roster file missing — cannot build structured players.")

# ─────────── INDEX AUX FEEDS ───────────
weather_by_team = {w["team"]: w["weather"] for w in weather}

box_by_pid = {str(b["player_id"]): b for b in boxscores}

starter_names = {normalize(g["home_pitcher"]) for g in starters} | \
                {normalize(g["away_pitcher"]) for g in starters}

team_to_gamepk, team_to_opp = {}, {}
for g in starters:
    gp = g["game_pk"]
    home, away = g["home_team_id"], g["away_team_id"]
    team_to_gamepk[home] = team_to_gamepk[away] = gp
    team_to_opp[home]    = away
    team_to_opp[away]    = home

bet_by_team = {o["team_id"]: o for o in odds}

# ESPN / Reddit mention counters
espn_cnt, reddit_cnt = Counter(), Counter()
for art in espn:
    title = str(art.get("headline", "")).lower()
    for r in rosters:
        if r["last_name"].lower() in title:
            espn_cnt[r["player_id"]] += 1
for post in reddit:
    t = str(post.get("title", "")).lower()
    for r in rosters:
        if r["last_name"].lower() in t:
            reddit_cnt[r["player_id"]] += 1

# ─────────── BUILD STRUCTURED PLAYERS ───────────
players_out = {}

for r in rosters:
    pid   = str(r["player_id"])
    name  = f'{r["first_name"]} {r["last_name"]}'
    team  = r["team"]
    tid   = r["team_id"]

    players_out[name] = {
        "player_id": pid,
        "name": name,
        "team": team,
        "team_id": tid,
        "position": r["position"],
        "handedness": {"bats": r["bats"], "throws": r["throws"]},
        "roster_status": {
            "status_code":        r["status_code"],
            "status_description": r["status_description"],
        },
        "starter": normalize(name) in starter_names if r["position"] == "P" else False,
        "opponent_team_id": team_to_opp.get(tid),
        "game_pk": team_to_gamepk.get(tid),
        "weather_context": weather_by_team.get(team),
        "betting_context": bet_by_team.get(tid),
        "espn_mentions":   espn_cnt.get(r["player_id"], 0),
        "reddit_mentions": reddit_cnt.get(r["player_id"], 0),
        "box_score":       box_by_pid.get(pid, {}),       # empty dict if none
    }

print(f"✅ Built structured entries for {len(players_out)} players.")

# ─────────── WRITE OUTPUT ───────────
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_out, f, ensure_ascii=False, indent=2)
print(f"💾 Wrote {OUT_FILE}")

# ─────────── OPTIONAL S3 PUSH ───────────
if UPLOAD_TO_S3:
    try:
        boto3.client("s3", region_name=REGION).upload_file(OUT_FILE, BUCKET, S3_KEY)
        print(f"☁️  Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ S3 upload failed: {e}")
