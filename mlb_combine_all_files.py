#!/usr/bin/env python3
import os
import re
import json
import glob
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import pytz

# ─── TIMEZONE-SAFE DATE ───
pst = pytz.timezone("US/Pacific")
DATE = os.getenv("FORCE_DATE", datetime.now(pst).strftime("%Y-%m-%d"))
YDAY = (datetime.strptime(DATE, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

# ─── PATH CONFIG ───
BASE         = "baseball"
ROSTER       = f"{BASE}/rosters/mlb_rosters_{DATE}.json"
STARTERS     = f"{BASE}/probablestarters/mlb_probable_starters_{DATE}.json"
WEATHER      = f"{BASE}/weather/mlb_weather_{DATE}.json"
ODDS         = f"{BASE}/betting/mlb_betting_odds_{DATE}.json"
ESPN         = f"{BASE}/news/mlb_espn_articles_{DATE}.json"
REDDIT_DIR   = "news-headlines-csvs/reddit_fantasy_baseball"
BOX          = f"{BASE}/boxscores/mlb_boxscores_{YDAY}.json"
OUT_FILE     = f"structured_players_{DATE}.json"

UPLOAD_TO_S3 = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"
BUCKET       = "fantasy-sports-csvs"
S3_KEY       = f"{BASE}/combined/{OUT_FILE}"
REGION       = "us-east-2"

# ─── HELPERS ───
def load_json(path):
    if not os.path.exists(path):
        print(f"⚠️ {path} not found — skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_all_reddit_jsons(date):
    files = glob.glob(os.path.join(REDDIT_DIR, f"*{date}*.json"))
    data = []
    for fp in files:
        data.extend(load_json(fp))
    return data

def normalize(text: str) -> str:
    return re.sub(r"[ .'-]", "", (text or "")).lower()

# ─── TEAM NAME MAP ───
TEAM_NAME_MAP = {}
TEAM_FILE = "team_map.json"
if os.path.exists(TEAM_FILE):
    with open(TEAM_FILE, "r") as f:
        for club in json.load(f):
            canon = club["name"]
            variants = {
                normalize(club["city"]),
                normalize(club["nick"]),
                normalize(f"{club['city']} {club['nick']}"),
                normalize(canon),
                normalize(club["abbr3"]),
                normalize(club["abbr2"]),
            }
            variants |= {v.replace(" ", "") for v in variants}
            for key in variants:
                TEAM_NAME_MAP[key] = canon

TEAM_NAME_MAP[normalize("As")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("A's")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("Sacramento Athletics")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("Sutter Health Park")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("Athletics")] = "Oakland Athletics"

# ─── LOAD DATA FILES ───
rosters   = load_json(ROSTER)
starters  = load_json(STARTERS)
weather   = load_json(WEATHER)
odds      = load_json(ODDS)
espn      = load_json(ESPN)
reddit    = load_all_reddit_jsons(DATE)
boxscores = load_json(BOX)

# ─── WEATHER LOOKUP ───
weather_by_team = {}
weather_grouped = defaultdict(list)
unmatched_weather_teams = set()
for rec in weather:
    raw_team = rec.get("team") or rec.get("team_name") or ""
    canon = TEAM_NAME_MAP.get(normalize(raw_team))
    if canon:
        weather_grouped[canon].append(rec)
    else:
        unmatched_weather_teams.add(raw_team)

for team, entries in weather_grouped.items():
    weather_by_team[team] = sorted(entries, key=lambda x: x.get("time_local", ""))[0]

if unmatched_weather_teams:
    print("❌ Unmatched teams in weather data:", unmatched_weather_teams)

# ─── MATCHUPS + BETTING ───
bet_by_team = defaultdict(lambda: {"over_under": None, "markets": []})
matchup_by_team = {}
unmatched_starter_teams = set()
for game in starters:
    raw_home = game.get("home_team", "")
    raw_away = game.get("away_team", "")
    canon_home = TEAM_NAME_MAP.get(normalize(raw_home), normalize(raw_home))
    canon_away = TEAM_NAME_MAP.get(normalize(raw_away), normalize(raw_away))

    if canon_home and canon_away:
        matchup_by_team[canon_home] = {
            "opponent": canon_away,
            "home_or_away": "home",
        }
        matchup_by_team[canon_away] = {
            "opponent": canon_home,
            "home_or_away": "away",
        }
    else:
        if not canon_home:
            unmatched_starter_teams.add(raw_home)
        if not canon_away:
            unmatched_starter_teams.add(raw_away)

if unmatched_starter_teams:
    print("❌ Unmatched teams in probable starters:", unmatched_starter_teams)

for o in odds:
    home_team = TEAM_NAME_MAP.get(normalize(o.get("home_team", "")))
    away_team = TEAM_NAME_MAP.get(normalize(o.get("away_team", "")))
    market = o.get("market")
    point = o.get("point")

    if home_team and away_team and market == "totals" and point is not None:
        bet_by_team[home_team]["over_under"] = point
        bet_by_team[away_team]["over_under"] = point

    for team in [home_team, away_team]:
        if team:
            bet_by_team[team]["markets"].append({
                "bookmaker": o.get("bookmaker"),
                "market": market,
                "odds": o.get("odds"),
                "point": point,
            })

# ─── STRUCTURE OUTPUT ───
players_out = {}
box_by_name = {normalize(b.get("Player Name", "")): b for b in boxscores}

espn_cnt = Counter()
espn_articles_by_pid = defaultdict(list)
for art in espn:
    hl, url = art.get("headline", ""), art.get("url", "")
    nh = normalize(hl)
    for r in rosters:
        pid = str(r["player_id"])
        if normalize(r["player"].split()[-1]) in nh:
            espn_cnt[pid] += 1
            espn_articles_by_pid[pid].append({"headline": hl, "url": url})

reddit_cnt = Counter()
for post in reddit:
    nt = normalize(post.get("title", ""))
    for r in rosters:
        pid = str(r["player_id"])
        if normalize(r["player"].split()[-1]) in nt:
            reddit_cnt[pid] += 1

starter_names = {
    normalize(game.get("home_pitcher", "")) for game in starters
} | {
    normalize(game.get("away_pitcher", "")) for game in starters
}

for r in rosters:
    pid = str(r["player_id"])
    name = r["player"].strip()
    raw_team = r.get("team", "")
    club = TEAM_NAME_MAP.get(normalize(raw_team), raw_team)
    club_key = normalize(club)

    wc = weather_by_team.get(club, {})
    matchup = matchup_by_team.get(club_key, {})
    bet = bet_by_team.get(club, {})

    is_pitcher = r.get("position") == "P"
    is_starter = normalize(name) in starter_names

    players_out[name] = {
        "player_id": pid,
        "name": name,
        "team": club,
        "opponent_team": matchup.get("opponent"),
        "home_or_away": matchup.get("home_or_away"),
        "position": r.get("position", ""),
        "handedness": {"bats": r.get("bats"), "throws": r.get("throws")},
        "roster_status": {
            "status_code": r.get("status_code"),
            "status_description": r.get("status_description"),
        },
        "is_probable_starter": is_starter,
        "starter": is_starter,
        "weather_context": wc,
        "betting_context": {
            "over_under": bet.get("over_under"),
            "markets": bet.get("markets", [])
        },
        "espn_mentions": espn_cnt.get(pid, 0),
        "espn_articles": espn_articles_by_pid.get(pid, []),
        "reddit_mentions": reddit_cnt.get(pid, 0),
        "box_score": box_by_name.get(normalize(name), {}),
    }

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_out, f, indent=2)
print(f"✅ Wrote {len(players_out)} players to {OUT_FILE}")

if UPLOAD_TO_S3:
    import boto3
    s3 = boto3.client("s3", region_name=REGION)
    try:
        s3.upload_file(OUT_FILE, BUCKET, S3_KEY)
        print(f"☁️ Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")