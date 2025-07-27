#!/usr/bin/env python3
import os
import re
import json
import sys
import boto3
import pytz
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# ───── DATE CONFIG ─────
pst = pytz.timezone("US/Pacific")
DATE = os.getenv("FORCE_DATE", datetime.now(pst).strftime("%Y-%m-%d"))
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

# ───── S3 SETTINGS ─────
UPLOAD_TO_S3 = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"
BUCKET       = "fantasy-sports-csvs"
S3_KEY       = f"{BASE}/combined/{OUT_FILE}"
REGION       = "us-east-1"

def load_json(path):
    if not os.path.exists(path):
        print(f"⚠️ {path} not found — skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize(name):
    return re.sub(r"[ .'-]", "", name).lower()

def split_name(fullname):
    parts = fullname.strip().split()
    return parts[0], " ".join(parts[1:]) if len(parts) > 1 else ""

# ───── LOAD FILES ─────
rosters   = load_json(FILE_ROSTER)
starters  = load_json(FILE_STARTERS)
weather   = load_json(FILE_WEATHER)
odds      = load_json(FILE_ODDS)
espn      = load_json(FILE_ESPN)
reddit    = load_json(FILE_REDDIT)
boxscores = load_json(FILE_BOX)

if not rosters:
    sys.exit("❌ Roster file missing — cannot proceed.")

# ───── INDEXES ─────
weather_by_team = {item["team"]: item for item in weather}
box_by_pid = {str(b.get("player_id") or b.get("id") or b.get("mlb_id")): b for b in boxscores}
starter_names = {normalize(g.get("home_pitcher", "")) for g in starters} | {normalize(g.get("away_pitcher", "")) for g in starters}

team_to_gamepk = {}
team_to_opp = {}
for g in starters:
    gp = g.get("game_pk")
    home = g.get("home_team_id")
    away = g.get("away_team_id")
    if gp and home and away:
        team_to_gamepk[home] = team_to_gamepk[away] = gp
        team_to_opp[home] = away
        team_to_opp[away] = home

bet_by_team = {}
for o in odds:
    tid = o.get("team_id") or o.get("teamId") or o.get("team")
    if tid:
        bet_by_team[str(tid)] = o

# ───── COUNT MENTIONS ─────
espn_cnt, reddit_cnt = Counter(), Counter()
for article in espn:
    title = article.get("headline", "").lower()
    for r in rosters:
        last_name = r.get("player", "").split()[-1].lower()
        if last_name in title:
            espn_cnt[r["player_id"]] += 1

for post in reddit:
    title = post.get("title", "").lower()
    for r in rosters:
        last_name = r.get("player", "").split()[-1].lower()
        if last_name in title:
            reddit_cnt[r["player_id"]] += 1

# ───── BUILD STRUCTURED OUTPUT ─────
players_out = {}

for r in rosters:
    pid = str(r["player_id"])
    full_name = r.get("player", "").strip()
    first_name, last_name = split_name(full_name)
    team = r.get("team")
    tid = r.get("team_id")

    if not full_name or not team:
        continue

    players_out[full_name] = {
        "player_id": pid,
        "name": full_name,
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
        "starter": normalize(full_name) in starter_names if r.get("position") == "P" else False,
        "opponent_team_id": team_to_opp.get(tid),
        "game_pk": team_to_gamepk.get(tid),
        "weather_context": weather_by_team.get(team),
        "betting_context": bet_by_team.get(str(tid)),
        "espn_mentions": espn_cnt.get(r["player_id"], 0),
        "reddit_mentions": reddit_cnt.get(r["player_id"], 0),
        "box_score": box_by_pid.get(pid, {})
    }

print(f"✅ Built structured entries for {len(players_out)} players.")

# ───── WRITE OUTPUT ─────
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_out, f, ensure_ascii=False, indent=2)
print(f"💾 Wrote file: {OUT_FILE}")

# ───── OPTIONAL UPLOAD TO S3 ─────
if UPLOAD_TO_S3:
    try:
        s3 = boto3.client("s3", region_name=REGION)
        s3.upload_file(OUT_FILE, BUCKET, S3_KEY)
        print(f"☁️ Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ Upload to S3 failed: {e}")
