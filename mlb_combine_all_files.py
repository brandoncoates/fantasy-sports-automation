
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

# Manual patch for "A's"
TEAM_NAME_MAP[normalize("A's")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("As")]  = "Oakland Athletics"

print("✅ TEAM_NAME_MAP ready.")

