#!/usr/bin/env python3
import os
import re
import json
import glob
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import pytz

# ───── TIMEZONE‑SAFE DATE ─────
pst = pytz.timezone("US/Pacific")
DATE = os.getenv("FORCE_DATE", datetime.now(pst).strftime("%Y-%m-%d"))
YDAY = (datetime.strptime(DATE, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

# ───── PATH CONFIG ─────
BASE         = "baseball"
ROSTER       = f"{BASE}/rosters/mlb_rosters_{DATE}.json"
STARTERS     = f"{BASE}/probablestarters/mlb_probable_starters_{DATE}.json"
WEATHER      = f"{BASE}/weather/mlb_weather_{DATE}.json"
ODDS         = f"{BASE}/betting/mlb_betting_odds_{DATE}.json"
ESPN         = f"{BASE}/news/mlb_espn_articles_{DATE}.json"
REDDIT_DIR   = "news-headlines-csvs/reddit_fantasy_baseball"
BOX          = f"{BASE}/boxscores/mlb_boxscores_{YDAY}.json"
OUT_FILE     = f"structured_players_{DATE}.json"

# ───── S3 CONFIG (OPTIONAL) ─────
UPLOAD_TO_S3 = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"
BUCKET       = "fantasy-sports-csvs"
S3_KEY       = f"{BASE}/combined/{OUT_FILE}"
REGION       = "us-east-2"

# ───── HELPERS ─────
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

# ───── TEAM NORMALIZATION ─────
mlb_clubs = [
    {"name":"Arizona Diamondbacks","city":"Arizona","nick":"Diamondbacks","abbr3":"ARI","abbr2":"AZ"},
    {"name":"Atlanta Braves","city":"Atlanta","nick":"Braves","abbr3":"ATL","abbr2":"GA"},
    {"name":"Baltimore Orioles","city":"Baltimore","nick":"Orioles","abbr3":"BAL","abbr2":"MD"},
    {"name":"Boston Red Sox","city":"Boston","nick":"Red Sox","abbr3":"BOS","abbr2":"MA"},
    {"name":"Chicago White Sox","city":"Chicago","nick":"White Sox","abbr3":"CHW","abbr2":"IL"},
    {"name":"Chicago Cubs","city":"Chicago","nick":"Cubs","abbr3":"CHC","abbr2":"IL"},
    {"name":"Cincinnati Reds","city":"Cincinnati","nick":"Reds","abbr3":"CIN","abbr2":"OH"},
    {"name":"Cleveland Guardians","city":"Cleveland","nick":"Guardians","abbr3":"CLE","abbr2":"OH"},
    {"name":"Colorado Rockies","city":"Colorado","nick":"Rockies","abbr3":"COL","abbr2":"CO"},
    {"name":"Detroit Tigers","city":"Detroit","nick":"Tigers","abbr3":"DET","abbr2":"MI"},
    {"name":"Houston Astros","city":"Houston","nick":"Astros","abbr3":"HOU","abbr2":"TX"},
    {"name":"Kansas City Royals","city":"Kansas City","nick":"Royals","abbr3":"KC","abbr2":"MO"},
    {"name":"Los Angeles Angels","city":"Los Angeles","nick":"Angels","abbr3":"LAA","abbr2":"CA"},
    {"name":"Los Angeles Dodgers","city":"Los Angeles","nick":"Dodgers","abbr3":"LAD","abbr2":"CA"},
    {"name":"Miami Marlins","city":"Miami","nick":"Marlins","abbr3":"MIA","abbr2":"FL"},
    {"name":"Milwaukee Brewers","city":"Milwaukee","nick":"Brewers","abbr3":"MIL","abbr2":"WI"},
    {"name":"Minnesota Twins","city":"Minnesota","nick":"Twins","abbr3":"MIN","abbr2":"MN"},
    {"name":"New York Mets","city":"New York","nick":"Mets","abbr3":"NYM","abbr2":"NY"},
    {"name":"New York Yankees","city":"New York","nick":"Yankees","abbr3":"NYY","abbr2":"NY"},
    {"name":"Oakland Athletics","city":"Oakland","nick":"Athletics","abbr3":"OAK","abbr2":"CA"},
    {"name":"Philadelphia Phillies","city":"Philadelphia","nick":"Phillies","abbr3":"PHI","abbr2":"PA"},
    {"name":"Pittsburgh Pirates","city":"Pittsburgh","nick":"Pirates","abbr3":"PIT","abbr2":"PA"},
    {"name":"San Diego Padres","city":"San Diego","nick":"Padres","abbr3":"SDP","abbr2":"CA"},
    {"name":"San Francisco Giants","city":"San Francisco","nick":"Giants","abbr3":"SFG","abbr2":"CA"},
    {"name":"Seattle Mariners","city":"Seattle","nick":"Mariners","abbr3":"SEA","abbr2":"WA"},
    {"name":"St. Louis Cardinals","city":"St. Louis","nick":"Cardinals","abbr3":"STL","abbr2":"MO"},
    {"name":"Tampa Bay Rays","city":"Tampa Bay","nick":"Rays","abbr3":"TB","abbr2":"FL"},
    {"name":"Texas Rangers","city":"Texas","nick":"Rangers","abbr3":"TEX","abbr2":"TX"},
    {"name":"Toronto Blue Jays","city":"Toronto","nick":"Blue Jays","abbr3":"TOR","abbr2":"ON"},
    {"name":"Washington Nationals","city":"Washington","nick":"Nationals","abbr3":"WSH","abbr2":"DC"},
]

TEAM_NAME_MAP = {}
for club in mlb_clubs:
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

# Add extra mappings
TEAM_NAME_MAP[normalize("As")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("A's")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("Sacramento Athletics")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("Sutter Health Park")] = "Oakland Athletics"

# ───── LOAD DATA FILES ─────
rosters   = load_json(ROSTER)
starters  = load_json(STARTERS)
weather   = load_json(WEATHER)
odds      = load_json(ODDS)
espn      = load_json(ESPN)
reddit    = load_all_reddit_jsons(DATE)
boxscores = load_json(BOX)

# ───── WEATHER LOOKUP — USE EARLIEST TIME IF MULTIPLE ─────
weather_by_team = {}
weather_grouped = defaultdict(list)
for rec in weather:
    raw_team = rec.get("team") or rec.get("team_name") or ""
    canon = TEAM_NAME_MAP.get(normalize(raw_team))
    if canon:
        weather_grouped[canon].append(rec)

for team, entries in weather_grouped.items():
    weather_by_team[team] = sorted(entries, key=lambda x: x.get("time_local", ""))[0]

# ───── BETTING LOOKUP — FIXED OVER/UNDER ─────
bet_by_team = defaultdict(lambda: {"over_under": None, "markets": []})
game_totals_seen = set()

for o in odds:
    home_team = o.get("home_team")
    away_team = o.get("away_team")
    team = o.get("team") or o.get("team_name", "")
    canon_team = TEAM_NAME_MAP.get(normalize(team))
    canon_home = TEAM_NAME_MAP.get(normalize(home_team))
    canon_away = TEAM_NAME_MAP.get(normalize(away_team))
    market = o.get("market")

    if canon_team:
        entry = {
            "bookmaker": o.get("bookmaker"),
            "market": market,
            "odds": o.get("odds"),
            "point": o.get("point"),
        }
        bet_by_team[canon_team]["markets"].append(entry)

    if market == "totals" and o.get("point") is not None and canon_home and canon_away:
        key = tuple(sorted([canon_home, canon_away]))
        if key not in game_totals_seen:
            bet_by_team[canon_home]["over_under"] = o["point"]
            bet_by_team[canon_away]["over_under"] = o["point"]
            game_totals_seen.add(key)

# ───── STARTERS + BOX ─────
box_by_name = { normalize(b.get("Player Name","")): b for b in boxscores }
starter_names = {
    normalize(g.get("home_pitcher","")) for g in starters
} | {
    normalize(g.get("away_pitcher","")) for g in starters
}

# ───── NEWS ─────
espn_cnt = Counter()
espn_articles_by_pid = defaultdict(list)
for art in espn:
    hl, url = art.get("headline",""), art.get("url","")
    nh = normalize(hl)
    for r in rosters:
        pid = str(r["player_id"])
        if normalize(r["player"].split()[-1]) in nh:
            espn_cnt[pid] += 1
            espn_articles_by_pid[pid].append({"headline": hl, "url": url})

reddit_cnt = Counter()
for post in reddit:
    nt = normalize(post.get("title",""))
    for r in rosters:
        pid = str(r["player_id"])
        if normalize(r["player"].split()[-1]) in nt:
            reddit_cnt[pid] += 1

# ───── STRUCTURE OUTPUT ─────
players_out = {}
for r in rosters:
    pid  = str(r["player_id"])
    name = r["player"].strip()
    raw_team = r.get("team", "")
    club = TEAM_NAME_MAP.get(normalize(raw_team), raw_team)

    wc = weather_by_team.get(club, {})
    weather_context = {
        "date": wc.get("date") or DATE,
        "team": wc.get("team") or club,
        "stadium": wc.get("stadium") or "",
        "time_local": wc.get("time_local") or "",
        "weather": wc.get("weather") or {},
        "precipitation_probability": wc.get("precipitation_probability") or 0,
        "cloud_cover_pct": wc.get("cloud_cover_pct") or 0,
        "weather_code": wc.get("weather_code") or "",
        "roof_type": wc.get("weather", {}).get("roof_status", "open") if isinstance(wc.get("weather"), dict) else "open"
    }

    bet = bet_by_team.get(club, {})
    players_out[name] = {
        "player_id": pid,
        "name": name,
        "team": club,
        "position": r.get("position", ""),
        "handedness": {"bats": r.get("bats"), "throws": r.get("throws")},
        "roster_status": {
            "status_code": r.get("status_code"),
            "status_description": r.get("status_description"),
        },
        "is_probable_starter": normalize(name) in starter_names if r.get("position") == "P" else False,
        "starter": r.get("position") != "P" and normalize(name) in starter_names,
        "weather_context": weather_context,
        "betting_context": {
            "over_under": bet.get("over_under"),
            "markets": bet.get("markets", [])
        },
        "espn_mentions": espn_cnt.get(pid, 0),
        "espn_articles": espn_articles_by_pid.get(pid, []),
        "reddit_mentions": reddit_cnt.get(pid, 0),
        "box_score": box_by_name.get(normalize(name), {}),
    }

# ───── SAVE FILE ─────
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players_out, f, indent=2)
print(f"✅ Wrote", len(players_out), "players to", OUT_FILE)

# ───── OPTIONAL UPLOAD ─────
if UPLOAD_TO_S3:
    import boto3
    s3 = boto3.client("s3", region_name=REGION)
    try:
        s3.upload_file(OUT_FILE, BUCKET, S3_KEY)
        print(f"☁️ Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
