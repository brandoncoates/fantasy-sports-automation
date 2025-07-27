#!/usr/bin/env python3
import os
import re
import json
import sys
import pytz
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# ───── DATE CONFIG ─────
pst = pytz.timezone("US/Pacific")
DATE = os.getenv("FORCE_DATE", datetime.now(pst).strftime("%Y-%m-%d"))
YDAY = (datetime.strptime(DATE, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

BASE = "baseball"
OUT_FILE = f"structured_players_{DATE}.json"

# ───── FILE PATHS ─────
FILE_ROSTER   = f"{BASE}/rosters/mlb_rosters_{DATE}.json"
FILE_STARTERS = f"{BASE}/probablestarters/mlb_probable_starters_{DATE}.json"
FILE_WEATHER  = f"{BASE}/weather/mlb_weather_{DATE}.json"
FILE_ODDS     = f"{BASE}/betting/mlb_betting_odds_{DATE}.json"
FILE_ESPN     = f"{BASE}/news/mlb_espn_articles_{DATE}.json"
FILE_REDDIT   = f"{BASE}/news/reddit_fantasybaseball_articles_{DATE}.json"
FILE_BOX      = f"{BASE}/boxscores/mlb_boxscores_{YDAY}.json"

# ───── HELPERS ─────
def load_json(path):
    if not os.path.exists(path):
        print(f"⚠️ {path} not found — skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize_name(name):
    return re.sub(r"[ .'-]", "", name).lower()

# Normalize short/alt team names
TEAM_NAME_MAP = {
    "Giants": "San Francisco Giants",
    "Dodgers": "Los Angeles Dodgers",
    "Yankees": "New York Yankees",
    "Mets": "New York Mets",
    "Red Sox": "Boston Red Sox",
    "White Sox": "Chicago White Sox",
    "Cubs": "Chicago Cubs",
    "A's": "Oakland Athletics",
    # Add other mappings as needed
}
def normalize_team(team):
    return TEAM_NAME_MAP.get(team, team)

# ───── LOAD FILES ─────
rosters   = load_json(FILE_ROSTER)
starters  = load_json(FILE_STARTERS)
weather   = load_json(FILE_WEATHER)
odds      = load_json(FILE_ODDS)
espn      = load_json(FILE_ESPN)
reddit    = load_json(FILE_REDDIT)
boxscores = load_json(FILE_BOX)

# ───── INDEXING ─────
weather_by_team = {normalize_team(w["team"]): w for w in weather if "team" in w}
box_by_pid = {
    str(b.get("player_id") or b.get("id") or b.get("mlb_id")): b
    for b in boxscores
}

starter_names = {
    normalize_name(g.get("home_pitcher", "")) for g in starters
}.union({
    normalize_name(g.get("away_pitcher", "")) for g in starters
})

team_to_gamepk, team_to_opp = {}, {}
for g in starters:
    game_pk = g.get("game_pk")
    home = g.get("home_team_id")
    away = g.get("away_team_id")
    if game_pk and home and away:
        team_to_gamepk[home] = game_pk
        team_to_gamepk[away] = game_pk
        team_to_opp[home] = away
        team_to_opp[away] = home

bet_by_team = {}
for o in odds:
    tid = str(o.get("team_id") or o.get("teamId") or o.get("team"))
    if tid:
        bet_by_team[tid] = o

# Mentions
espn_mentions = defaultdict(list)
reddit_mentions = defaultdict(list)

for article in espn:
    title = article.get("headline", "").lower()
    for player in rosters:
        if player["last_name"].lower() in title:
            espn_mentions[player["player_id"]].append(article)

for post in reddit:
    title = post.get("title", "").lower()
    for player in rosters:
        if player["last_name"].lower() in title:
            reddit_mentions[player["player_id"]].append(post)

# ───── COMBINE STRUCTURED PLAYER OUTPUT ─────
players_out = {}

for player in rosters:
    pid = str(player["player_id"])
    first = player.get("first_name", "").strip()
    last = player.get("last_name", "").strip()
    name = f"{first} {last}".strip()
    if not name or not player.get("team"):
        continue

    team_name = normalize_team(player["team"])
    tid = player.get("team_id")
    position = player.get("position", "")

    players_out[pid] = {
        "player_id": pid,
        "name": name,
        "team": team_name,
        "team_id": tid,
        "position": position,
        "handedness": {
            "bats": player.get("bats"),
            "throws": player.get("throws"),
        },
        "roster_status": {
            "status_code": player.get("status_code"),
            "status_description": player.get("status_description"),
        },
        "starter": normalize_name(name) in starter_names if position == "P" else False,
        "opponent_team_id": team_to_opp.get(tid),
        "game_pk": team_to_gamepk.get(tid),
        "weather_context": weather_by_team.get(team_name),
        "betting_context": bet_by_team.get(str(tid)),
        "espn_mentions": espn_mentions.get(player["player_id"], []),
        "reddit_mentions": reddit_mentions.get(player["player_id"], []),
        "box_score": box_by_pid.get(pid, {}),
    }

# ───── WRITE OUTPUT ─────
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_out, f, ensure_ascii=False, indent=2)

print(f"✅ Combined {len(players_out)} players into {OUT_FILE}")
