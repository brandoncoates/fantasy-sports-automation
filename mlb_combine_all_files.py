#!/usr/bin/env python3
"""
mlb_combine_all_files.py
────────────────────────
Merge daily JSON feeds and write structured_players_{DATE}.json
"""

import os
import re
import json
import sys
import boto3
from datetime import datetime, timedelta

# ───── DATE CONFIG ─────
DATE = os.getenv("FORCE_DATE", datetime.now().strftime("%Y-%m-%d"))
YDAY = (datetime.strptime(DATE, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

BASE = os.getenv("COMBINE_BASE_DIR", "baseball")

# ───── FILE PATHS ─────
FILE_ROSTER   = f"{BASE}/rosters/mlb_rosters_{DATE}.json"
FILE_STARTERS = f"{BASE}/probablestarters/mlb_probable_starters_{DATE}.json"
FILE_WEATHER  = f"{BASE}/weather/mlb_weather_{DATE}.json"
FILE_ODDS     = f"{BASE}/betting/mlb_betting_odds_{DATE}.json"
FILE_ESPN     = f"{BASE}/news/mlb_espn_articles_{DATE}.json"
FILE_REDDIT   = f"{BASE}/news/reddit_fantasybaseball_articles_{DATE}.json"
FILE_BOX      = f"{BASE}/boxscores/mlb_boxscores_{YDAY}.json"

OUT_FILE = f"structured_players_{DATE}.json"
S3_KEY   = f"{BASE}/combined/{OUT_FILE}"

# ───── CONDITIONAL S3 UPLOAD ─────
UPLOAD_TO_S3 = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"
BUCKET       = os.getenv("S3_BUCKET_NAME", "fantasy-sports-csvs")
REGION       = os.getenv("AWS_REGION", "us-east-1")

# ───── HELPERS ─────
def load_json(path):
    if not os.path.exists(path):
        print(f"⚠️  Missing: {path}")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize(name: str) -> str:
    return re.sub(r"[ .'-]", "", name).lower()

# ───── LOAD DATA ─────
rosters   = load_json(FILE_ROSTER)
starters  = load_json(FILE_STARTERS)
weather   = load_json(FILE_WEATHER)
odds      = load_json(FILE_ODDS)
espn      = load_json(FILE_ESPN)
reddit    = load_json(FILE_REDDIT)
boxscores = load_json(FILE_BOX)

if not rosters:
    sys.exit("❌ Roster file missing or empty — cannot continue.")

# ───── INDEX FEEDS ─────
weather_by_team = {w.get("team"): w for w in weather}
box_by_pid = {str(b.get("player_id")): b for b in boxscores}

starter_names = {
    normalize(g.get("home_pitcher", "")) for g in starters
}.union({
    normalize(g.get("away_pitcher", "")) for g in starters
})

team_to_gamepk, team_to_opp = {}, {}
for g in starters:
    gp, home, away = g.get("game_pk"), g.get("home_team_id"), g.get("away_team_id")
    if gp and home and away:
        team_to_gamepk[home] = team_to_gamepk[away] = gp
        team_to_opp[home] = away
        team_to_opp[away] = home

bet_by_team = {}
for o in odds:
    tid = o.get("team_id") or o.get("teamId") or o.get("team")
    if tid:
        bet_by_team[tid] = o

espn_cnt, reddit_cnt = {}, {}
for art in espn:
    title = str(art.get("headline", "")).lower()
    for r in rosters:
        last = r.get("player", "").split()[-1].lower()
        if last in title:
            espn_cnt[r["player_id"]] = espn_cnt.get(r["player_id"], 0) + 1

for post in reddit:
    txt = str(post.get("title", "")).lower()
    for r in rosters:
        last = r.get("player", "").split()[-1].lower()
        if last in txt:
            reddit_cnt[r["player_id"]] = reddit_cnt.get(r["player_id"], 0) + 1

# ───── BUILD STRUCTURED OUTPUT ─────
players_out = {}

for r in rosters:
    pid   = str(r.get("player_id"))
    name  = r.get("player", "")
    team  = r.get("team", "")
    tid   = r.get("team_id")

    players_out[name] = {
        "player_id": pid,
        "name": name,
        "team": team,
        "team_id": tid,
        "position": r.get("position", ""),
        "handedness": {
            "bats": r.get("bats", ""),
            "throws": r.get("throws", "")
        },
        "roster_status": {
            "status_code": r.get("status_code", ""),
            "status_description": r.get("status_description", "")
        },
        "starter": normalize(name) in starter_names if r.get("position") == "P" else False,
        "opponent_team_id": team_to_opp.get(tid),
        "game_pk": team_to_gamepk.get(tid),
        "weather_context": weather_by_team.get(team),
        "betting_context": bet_by_team.get(tid),
        "espn_mentions": espn_cnt.get(r["player_id"], 0),
        "reddit_mentions": reddit_cnt.get(r["player_id"], 0),
        "box_score": box_by_pid.get(pid, {}),
    }

print(f"✅ Built structured entries for {len(players_out)} players.")

# ───── WRITE FILE LOCALLY ─────
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_out, f, indent=2)
print(f"💾 Wrote: {OUT_FILE}")

# ───── OPTIONAL S3 UPLOAD ─────
if UPLOAD_TO_S3:
    try:
        boto3.client("s3", region_name=REGION).upload_file(OUT_FILE, BUCKET, S3_KEY)
        print(f"☁️  Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ S3 upload failed: {e}")
