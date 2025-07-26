#!/usr/bin/env python3
"""
mlb_combine_all_files.py
────────────────────────
Create ONE master structured‑players JSON for today's slate by merging:

  • rosters/mlb_rosters_{DATE}.json              ← REQUIRED
  • probablestarters/mlb_probable_starters_{DATE}.json
  • weather/mlb_weather_{DATE}.json
  • betting/mlb_betting_odds_{DATE}.json
  • news/mlb_espn_articles_{DATE}.json
  • news/reddit_fantasybaseball_articles_{DATE}.json
  • boxscores/mlb_boxscores_{YESTERDAY}.json     ← yesterday’s results for recap

Output
  structured_players_{DATE}.json   (written to repo root)
"""

import os, re, json, sys, boto3
from datetime import datetime, timedelta
from collections import Counter, defaultdict

# ───── DATES ─────
DATE   = datetime.now().strftime("%Y-%m-%d")          # e.g., 2025‑07‑26
YDAY   = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# ───── FILE PATHS (match S3 folder layout) ─────
FILE_ROSTER   = f"rosters/mlb_rosters_{DATE}.json"
FILE_STARTERS = f"probablestarters/mlb_probable_starters_{DATE}.json"
FILE_WEATHER  = f"weather/mlb_weather_{DATE}.json"
FILE_ODDS     = f"betting/mlb_betting_odds_{DATE}.json"
FILE_ESPN     = f"news/mlb_espn_articles_{DATE}.json"
FILE_REDDIT   = f"news/reddit_fantasybaseball_articles_{DATE}.json"
FILE_BOX      = f"boxscores/mlb_boxscores_{YDAY}.json"

OUT_FILE      = f"structured_players_{DATE}.json"

# Optional S3 push (set True after testing)
UPLOAD_TO_S3  = False
BUCKET        = "fantasy-sports-csvs"
S3_KEY        = f"baseball/combined/{OUT_FILE}"
REGION        = "us-east-1"

# ───── HELPERS ─────
def load_json(path):
    if not os.path.exists(path):
        print(f"⚠️  {path} not found — skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def norm(name: str) -> str:
    return re.sub(r"[ .'-]", "", name).lower()

def idx(items, key):
    return {str(it[key]): it for it in items if key in it}

# ───── LOAD RAW FEEDS ─────
rosters   = load_json(FILE_ROSTER)      # REQUIRED
starters  = load_json(FILE_STARTERS)
weather   = load_json(FILE_WEATHER)
odds      = load_json(FILE_ODDS)
espn      = load_json(FILE_ESPN)
reddit    = load_json(FILE_REDDIT)
boxscores = load_json(FILE_BOX)

if not rosters:
    sys.exit("❌ Roster file missing — cannot build structured players.")

# ───── QUICK INDICES ─────
weather_by_team = {w["team"]: w["weather"] for w in weather}
box_by_pid      = idx(boxscores, "player_id")

# Probable starters → set of normalized pitcher names
starter_names = {norm(g["home_pitcher"]) for g in starters} | \
                {norm(g["away_pitcher"]) for g in starters}

# Build opponent / gamePk maps from starters
team_to_gamepk, team_to_opp = {}, {}
for g in starters:
    game_pk, home_id, away_id = g["game_pk"], g["home_team_id"], g["away_team_id"]
    team_to_gamepk[home_id] = team_to_gamepk[away_id] = game_pk
    team_to_opp[home_id]    = away_id
    team_to_opp[away_id]    = home_id

# Betting odds per team_id
bet_by_team = defaultdict(dict)
for o in odds:
    team_id = o["team_id"]
    bet_by_team[team_id].update(o)

# Mentions counters (surname match)
espn_cnt, reddit_cnt = Counter(), Counter()
for art in espn:
    title = art["headline"].lower()
    for r in rosters:
        if r["last_name"].lower() in title:
            espn_cnt[r["player_id"]] += 1
for post in reddit:
    t = post["title"].lower()
    for r in rosters:
        if r["last_name"].lower() in t:
            reddit_cnt[r["player_id"]] += 1

# ───── BUILD STRUCTURED PLAYERS ─────
players_out = {}

for r in rosters:
    pid    = str(r["player_id"])
    name   = f'{r["first_name"]} {r["last_name"]}'
    team   = r["team"]
    teamid = r["team_id"]

    players_out[name] = {
        "player_id": pid,
        "name": name,
        "team": team,
        "team_id": teamid,
        "position": r["position"],
        "handedness": {"bats": r["bats"], "throws": r["throws"]},
        "roster_status": {
            "status_code":        r["status_code"],
            "status_description": r["status_description"],
        },
        "starter": norm(name) in starter_names if r["position"] == "P" else False,
        "opponent_team_id": team_to_opp.get(teamid),
        "game_pk": team_to_gamepk.get(teamid),
        "weather_context": weather_by_team.get(team),
        "betting_context": bet_by_team.get(teamid),
        "espn_mentions":   espn_cnt.get(pid, 0),
        "reddit_mentions": reddit_cnt.get(pid, 0),
        "box_score":       box_by_pid.get(pid, {}),
    }

print(f"✅ Built structured entries for {len(players_out)} players.")

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
