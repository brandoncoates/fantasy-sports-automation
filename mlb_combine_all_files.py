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

BASE = "baseball"

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

# ───── HELPERS ─────
def load_json(path):
    if not os.path.exists(path):
        print(f"⚠️  {path} not found — skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize(text):
    return re.sub(r"[^a-z0-9]", "", text.lower()) if isinstance(text, str) else ""

# ───── LOAD FILES ─────
rosters   = load_json(FILE_ROSTER)
starters  = load_json(FILE_STARTERS)
weather   = load_json(FILE_WEATHER)
odds      = load_json(FILE_ODDS)
espn      = load_json(FILE_ESPN)
reddit    = load_json(FILE_REDDIT)
boxscores = load_json(FILE_BOX)

if not rosters:
    sys.exit("❌ Roster file missing — cannot build structured players.")

# ───── INDEX SUPPORT FILES ─────
starter_names = set()
for game in starters:
    for p in ("home_pitcher", "away_pitcher"):
        if game.get(p):
            starter_names.add(normalize(game[p]))

team_weather_map = {normalize(w.get("team")): w for w in weather}
team_odds_map = {normalize(o.get("team_name") or o.get("team") or o.get("team_id")): o for o in odds}

team_to_gamepk, team_to_opp = {}, {}
for g in starters:
    home = normalize(g.get("home_team_name", ""))
    away = normalize(g.get("away_team_name", ""))
    pk = g.get("game_pk")
    if home and away and pk:
        team_to_gamepk[home] = pk
        team_to_gamepk[away] = pk
        team_to_opp[home] = away
        team_to_opp[away] = home

box_by_pid = {str(p.get("player_id") or p.get("id") or p.get("mlb_id")): p for p in boxscores}

# Mentions
espn_cnt = Counter()
reddit_cnt = Counter()
for art in espn:
    title = normalize(art.get("headline", ""))
    for r in rosters:
        last = normalize(r["player"].split()[-1])
        if last and last in title:
            espn_cnt[r["player_id"]] += 1
for post in reddit:
    title = normalize(post.get("title", ""))
    for r in rosters:
        last = normalize(r["player"].split()[-1])
        if last and last in title:
            reddit_cnt[r["player_id"]] += 1

# ───── COMBINE DATA ─────
players_out = {}

for r in rosters:
    pid = str(r["player_id"])
    name = r["player"]
    team = r["team"]
    pos = r.get("position")
    norm_name = normalize(name)
    norm_team = normalize(team)

    players_out[name] = {
        "player_id": pid,
        "name": name,
        "team": team,
        "position": pos,
        "handedness": {
            "bats": r.get("bats", ""),
            "throws": r.get("throws", "")
        },
        "roster_status": {
            "status_code": r.get("status_code", ""),
            "status_description": r.get("status_description", "")
        },
        "starter": norm_name in starter_names if pos == "P" else False,
        "opponent_team": team_to_opp.get(norm_team),
        "game_pk": team_to_gamepk.get(norm_team),
        "weather_context": team_weather_map.get(norm_team),
        "betting_context": team_odds_map.get(norm_team),
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
