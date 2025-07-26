#!/usr/bin/env python3
"""
mlb_combine_all_files.py
────────────────────────
Build ONE master player file for a slate by stitching together:

  • mlb_rosters_{DATE}.json
  • mlb_probable_starters_{DATE}.json
  • mlb_weather_{DATE}.json
  • mlb_betting_odds_{DATE}.json
  • mlb_espn_articles_{DATE}.json
  • reddit_fantasybaseball_articles_{DATE}.json
  • mlb_boxscores_{YESTERDAY}.json  (results for recap)

Output:
  structured_players_{DATE}.json   (ready for recap + picks)
"""

import os, re, json, sys, boto3
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# ───── CONFIG ─────
DATE          = datetime.now().strftime("%Y-%m-%d")          # slate date
YDAY          = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

FILE_ROSTER   = f"mlb_rosters_{DATE}.json"
FILE_STARTERS = f"mlb_probable_starters_{DATE}.json"
FILE_WEATHER  = f"mlb_weather_{DATE}.json"
FILE_ODDS     = f"mlb_betting_odds_{DATE}.json"
FILE_ESPN     = f"mlb_espn_articles_{DATE}.json"
FILE_REDDIT   = f"reddit_fantasybaseball_articles_{DATE}.json"
FILE_BOX      = f"mlb_boxscores_{YDAY}.json"

OUT_FILE      = f"structured_players_{DATE}.json"

UPLOAD_TO_S3  = False
BUCKET        = "fantasy-sports-csvs"
S3_KEY        = f"baseball/structured/{OUT_FILE}"
REGION        = "us-east-1"

# ───── HELPERS ─────
def load(path):
    if not os.path.exists(path):
        print(f"⚠️  {path} not found — skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def norm(name: str) -> str:
    return re.sub(r"[ .'-]", "", name).lower()

def idx(items, key):
    return {str(x[key]): x for x in items if key in x}

# ───── LOAD RAW FEEDS ─────
rosters   = load(FILE_ROSTER)        # <<< BASE LIST
starters  = load(FILE_STARTERS)
weather   = load(FILE_WEATHER)
odds      = load(FILE_ODDS)
espn      = load(FILE_ESPN)
reddit    = load(FILE_REDDIT)
boxscores = load(FILE_BOX)

if not rosters:
    sys.exit("❌ Roster file missing — can't build structured players.")

# ───── QUICK INDICES ─────
weather_by_team  = {w["team"]: w["weather"] for w in weather}
box_by_pid       = idx(boxscores, "player_id")

# Build game & opponent maps from starters feed
team_to_gamepk, team_to_opp = {}, {}
starter_set = set()
for g in starters:
    game_pk = g["game_pk"]
    home_id, away_id = g["home_team_id"], g["away_team_id"]
    team_to_gamepk[home_id] = team_to_gamepk[away_id] = game_pk
    team_to_opp[home_id]    = away_id
    team_to_opp[away_id]    = home_id
    starter_set.add(norm(g["home_pitcher"]))
    starter_set.add(norm(g["away_pitcher"]))

# Betting odds collapsed by team_id
bet_by_team = {}
for o in odds:
    team_id = o["team_id"]
    bet_by_team.setdefault(team_id, {}).update(o)

# Mentions counters (surname match)
espn_cnt, reddit_cnt = Counter(), Counter()
for art in espn:
    title = art["headline"].lower()
    for p in rosters:
        if p["last_name"].lower() in title:
            espn_cnt[p["player_id"]] += 1
for post in reddit:
    t = post["title"].lower()
    for p in rosters:
        if p["last_name"].lower() in t:
            reddit_cnt[p["player_id"]] += 1

# ───── BUILD STRUCTURED PLAYERS ─────
players_out = {}

for p in rosters:
    pid   = str(p["player_id"])
    name  = f'{p["first_name"]} {p["last_name"]}'
    team  = p["team"]
    tid   = p["team_id"]

    player = {
        "player_id": pid,
        "name": name,
        "team": team,
        "team_id": tid,
        "position": p["position"],
        "handedness": {"bats": p["bats"], "throws": p["throws"]},
        "roster_status": {
            "status_code":        p["status_code"],
            "status_description": p["status_description"],
        },
        "starter": norm(name) in starter_set if p["position"] == "P" else False,
        "opponent_team_id": team_to_opp.get(tid),
        "game_pk": team_to_gamepk.get(tid),
        "weather_context": weather_by_team.get(team),
        "betting_context": bet_by_team.get(tid),
        # placeholders that downstream scripts may overwrite later
        "espn_mentions": espn_cnt.get(pid, 0),
        "reddit_mentions": reddit_cnt.get(pid, 0),
        "box_score": box_by_pid.get(pid, {}),
    }

    players_out[name] = player

print(f"✅ Created structured records for {len(players_out)} players.")

# ───── WRITE OUTPUT ─────
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_out, f, ensure_ascii=False, indent=2)
print(f"💾 Wrote {OUT_FILE}")

# ───── OPTIONAL S3 UPLOAD ─────
if UPLOAD_TO_S3:
    try:
        boto3.client("s3", region_name=REGION).upload_file(OUT_FILE, BUCKET, S3_KEY)
        print(f"☁️  Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ S3 upload failed: {e}")
