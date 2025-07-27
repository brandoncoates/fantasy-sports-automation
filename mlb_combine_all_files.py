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
YDAY = (datetime.strptime(DATE, "%Y‑%m‑%d") - timedelta(days=1)).strftime("%Y-%m-%d")

# ───── PATH CONFIG ─────
BASE       = "baseball"
ROSTER     = f"{BASE}/rosters/mlb_rosters_{DATE}.json"
STARTERS   = f"{BASE}/probablestarters/mlb_probable_starters_{DATE}.json"
WEATHER    = f"{BASE}/weather/mlb_weather_{DATE}.json"
ODDS       = f"{BASE}/betting/mlb_betting_odds_{DATE}.json"
ESPN       = f"{BASE}/news/mlb_espn_articles_{DATE}.json"
REDDIT_DIR = f"{BASE}/news-headlines-csvs/reddit_fantasy_baseball"
BOX        = f"{BASE}/boxscores/mlb_boxscores_{YDAY}.json"

OUT_FILE   = f"structured_players_{DATE}.json"

# ───── S3 CONFIG (OPTIONAL) ─────
UPLOAD_TO_S3 = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"
BUCKET       = "fantasy-sports-csvs"
S3_KEY       = f"{BASE}/combined/{OUT_FILE}"
REGION       = "us-east-1"

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

# ───── DYNAMIC TEAM NAME MAPPING ─────
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

# ───── LOAD DATA FILES ─────
rosters   = load_json(ROSTER)
starters  = load_json(STARTERS)
weather   = load_json(WEATHER)
odds      = load_json(ODDS)
espn      = load_json(ESPN)
reddit    = load_all_reddit_jsons(DATE)
boxscores = load_json(BOX)

# ───── BUILD WEATHER LOOKUP ─────
# away‑team entries
weather_by_away = {
    TEAM_NAME_MAP.get(normalize(w["team"]), w["team"]): w
    for w in weather if w.get("team")
}
# copy to home teams via probables
weather_by_team = dict(weather_by_away)
for g in starters:
    away = TEAM_NAME_MAP.get(normalize(g.get("away_team","")), g.get("away_team",""))
    home = TEAM_NAME_MAP.get(normalize(g.get("home_team","")), g.get("home_team",""))
    if away in weather_by_away:
        weather_by_team[home] = weather_by_away[away]

# ───── BUILD INDEXES ─────
box_by_name = { normalize(bs.get("Player Name","")): bs for bs in boxscores }

starter_names = {
    normalize(g.get("home_pitcher","")) for g in starters
} | {
    normalize(g.get("away_pitcher","")) for g in starters
}

bet_by_team = {}
for o in odds:
    team_raw  = o.get("team") or o.get("team_name","")
    team_norm = TEAM_NAME_MAP.get(normalize(team_raw), team_raw)
    bet_by_team[team_norm] = o

espn_articles_by_pid = defaultdict(list)
espn_cnt = Counter()
for art in espn:
    hl = art.get("headline","")
    url= art.get("url","")
    nh = normalize(hl)
    for r in rosters:
        pid = str(r["player_id"])
        if normalize(r["player"].split()[-1]) in nh:
            espn_cnt[pid]+=1
            espn_articles_by_pid[pid].append({"headline":hl,"url":url})

reddit_cnt = Counter()
for post in reddit:
    nt = normalize(post.get("title",""))
    for r in rosters:
        pid = str(r["player_id"])
        if normalize(r["player"].split()[-1]) in nt:
            reddit_cnt[pid]+=1

# ───── BUILD STRUCTURED OUTPUT ─────
players_out = {}
for r in rosters:
    pid      = str(r["player_id"])
    name     = r["player"].strip()
    team_raw = r.get("team","")
    pos      = r.get("position","")
    team_can = TEAM_NAME_MAP.get(normalize(team_raw), team_raw)

    wc = weather_by_team.get(team_can, {})
    weather_context = {
        "date":                      wc.get("date"),
        "team":                      wc.get("team"),
        "stadium":                   wc.get("stadium"),
        "time_local":                wc.get("time_local"),
        "weather":                   wc.get("weather"),
        "precipitation_probability": wc.get("precipitation_probability"),
        "cloud_cover_pct":           wc.get("cloud_cover_pct"),
        "weather_code":              wc.get("weather_code"),
        "roof_type":                 (wc.get("weather") or {}).get("roof_status","open"),
    }

    bs = box_by_name.get(normalize(name), {})

    players_out[name] = {
        "player_id":      pid,
        "name":           name,
        "team":           team_can,
        "position":       pos,
        "handedness":     {"bats": r.get("bats"), "throws": r.get("throws")},
        "roster_status":  {"status_code": r.get("status_code"), "status_description": r.get("status_description")},
        "starter":        normalize(name) in starter_names if pos=="P" else False,
        "weather_context":weather_context,
        "betting_context":bet_by_team.get(team_can, {}),
        "espn_mentions":  espn_cnt.get(pid,0),
        "espn_articles":  espn_articles_by_pid.get(pid,[]),
        "reddit_mentions":reddit_cnt.get(pid,0),
        "box_score":      bs,
    }

# ───── SAVE FILE ─────
with open(OUT_FILE,"w",encoding="utf-8") as f:
    json.dump(players_out,f,indent=2)
print(f"✅ Wrote {len(players_out)} player entries to {OUT_FILE}")

# ───── OPTIONAL UPLOAD ─────
if UPLOAD_TO_S3:
    import boto3
    s3 = boto3.client("s3",region_name=REGION)
    try:
        s3.upload_file(OUT_FILE,BUCKET,S3_KEY)
        print(f"☁️ Uploaded to s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
