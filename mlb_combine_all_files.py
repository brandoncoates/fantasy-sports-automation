#!/usr/bin/env python3
"""
mlb_combine_all_files.py
────────────────────────
Fuse every daily JSON feed (players, box scores, weather, betting odds,
ESPN & Reddit mentions, probable starters, rosters) into ONE master file.

• Reads files whose names follow the pattern produced by your scrapers.
• Populates / overwrites the placeholder blocks inside the player objects:
    - box_score
    - weather_context
    - betting_context
    - roster_status
    - handedness
    - espn_mentions
    - reddit_mentions
    - starter  (for pitchers)

Output:
    merged_players_YYYY-MM-DD.json  in the working directory
    (optional) automatic upload to S3

Adjust file‑name patterns or S3 settings at the CONFIG section.
"""

import os
import re
import json
import sys
import boto3
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# ───────── CONFIG ─────────
SLATE_DATE   = datetime.now().strftime("%Y-%m-%d")      # e.g. 2025-07-26
BOX_DATE     = (datetime.strptime(SLATE_DATE, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

FILE_STRUCT  = f"mlb_structured_players_{SLATE_DATE}.json"
FILE_BOX     = f"mlb_boxscores_{BOX_DATE}.json"
FILE_WEATHER = f"mlb_weather_{SLATE_DATE}.json"
FILE_ODDS    = f"mlb_betting_odds_{SLATE_DATE}.json"
FILE_ESPN    = f"mlb_espn_articles_{SLATE_DATE}.json"
FILE_REDDIT  = f"reddit_fantasybaseball_articles_{SLATE_DATE}.json"
FILE_STARTER = f"mlb_probable_starters_{SLATE_DATE}.json"
FILE_ROSTER  = f"mlb_rosters_{SLATE_DATE}.json"

OUTPUT_FILE  = f"merged_players_{SLATE_DATE}.json"

# Set to True when you’re ready to push automatically
UPLOAD_TO_S3 = False
BUCKET       = "fantasy-sports-csvs"
S3_KEY       = f"baseball/merged/{OUTPUT_FILE}"
REGION       = "us-east-1"

# ───────── HELPERS ─────────
def load_json(path):
    if not os.path.exists(path):
        print(f"⚠️  {path} not found – skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def index_by(items, key):
    """Return dict keyed by items[*][key] (as str)."""
    return {str(it[key]): it for it in items if key in it}

def normalize_name(name: str) -> str:
    """Lowercase and strip whitespace / punctuation for fuzzy matches."""
    return re.sub(r"[ .'-]", "", name).lower()

# ───────── LOAD FEEDS ─────────
players_dict   = load_json(FILE_STRUCT)          # dict keyed by player **name**
box_list       = load_json(FILE_BOX)
weather_list   = load_json(FILE_WEATHER)
odds_list      = load_json(FILE_ODDS)
espn_list      = load_json(FILE_ESPN)
reddit_list    = load_json(FILE_REDDIT)
starter_list   = load_json(FILE_STARTER)
roster_list    = load_json(FILE_ROSTER)

if not players_dict:
    sys.exit("❌ Base structured‑players file missing; aborting combine step.")

# ─── Index side feeds ───
box_by_name     = {normalize_name(b["Player Name"]): b for b in box_list}

weather_by_team = {w["team"]: w["weather"] for w in weather_list}

# Betting odds: moneyline / OU collapsed to one dict per team
betting_by_team = defaultdict(dict)
for row in odds_list:
    team = row["team"]
    mkt  = row["market"]
    if mkt == "h2h":        # moneyline (decimal odds)
        betting_by_team[team]["moneyline_decimal"] = row["odds"]
    elif mkt == "totals" and row["team"].lower() in ("over", "under"):
        matchup_key = f"{row['home_team']} vs {row['away_team']}"
        betting_by_team[matchup_key]["over_under"] = row["point"]

# Probable starter names (normalized)
starter_name_set = {normalize_name(d["home_pitcher"]) for d in starter_list} | \
                   {normalize_name(d["away_pitcher"]) for d in starter_list}

# Roster info by player_id
roster_by_pid = index_by(roster_list, "player_id")

# Mentions counters
espn_counter   = Counter()
for art in espn_list:
    title = art["headline"]
    for name in players_dict:
        if name.split()[-1].lower() in title.lower():
            espn_counter[name] += 1

reddit_counter = Counter()
for post in reddit_list:
    title = post["title"]
    for name in players_dict:
        if name.split()[-1].lower() in title.lower():
            reddit_counter[name] += 1

# ───────── MERGE LOOP ─────────
updated = 0
for name, player in players_dict.items():
    norm_name = normalize_name(name)
    team      = player.get("team")

    # 1) box score
    if norm_name in box_by_name:
        player["box_score"] = box_by_name[norm_name]

    # 2) weather
    if team in weather_by_team:
        player["weather_context"] = weather_by_team[team]

    # 3) betting odds
    if team in betting_by_team:
        player["betting_context"] = betting_by_team[team]

    # 4) starter flag refresh
    if player.get("position") == "P":
        player["starter"] = norm_name in starter_name_set

    # 5) roster status & handedness
    pid = str(player.get("player_id"))
    if pid in roster_by_pid:
        r = roster_by_pid[pid]
        player["roster_status"] = {
            "status_code":        r["status_code"],
            "status_description": r["status_description"]
        }
        player["handedness"] = {
            "bats":   r["bats"],
            "throws": r["throws"]
        }

    # 6) media mentions
    if espn_counter[name]:
        player["espn_mentions"] = espn_counter[name]
    if reddit_counter[name]:
        player["reddit_mentions"] = reddit_counter[name]

    updated += 1

print(f"✅ Populated/updated {updated} player entries.")

# ───────── WRITE OUTPUT ─────────
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_dict, f, ensure_ascii=False, indent=2)
print(f"💾 Wrote {OUTPUT_FILE}")

# ───────── OPTIONAL S3 UPLOAD ─────────
if UPLOAD_TO_S3:
    try:
        boto3.client("s3", region_name=REGION).upload_file(OUTPUT_FILE, BUCKET, S3_KEY)
        print(f"☁️  Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ S3 upload failed: {e}")
