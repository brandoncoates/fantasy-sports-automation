#!/usr/bin/env python3
import os, re, json, sys, boto3
from datetime import datetime, timedelta
from collections import defaultdict, Counter

DATE = os.getenv("FORCE_DATE", datetime.now().strftime("%Y-%m-%d"))
YDAY = (datetime.strptime(DATE, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
BASE = "baseball"

def path(folder, fn): return f"{BASE}/{folder}/{fn}"

FILE_ROSTER   = path("rosters",              f"mlb_rosters_{DATE}.json")
FILE_STARTERS = path("probablestarters",     f"mlb_probable_starters_{DATE}.json")
FILE_WEATHER  = path("weather",              f"mlb_weather_{DATE}.json")
FILE_ODDS     = path("betting",              f"mlb_betting_odds_{DATE}.json")
FILE_ESPN     = path("news",                 f"mlb_espn_articles_{DATE}.json")
FILE_REDDIT   = path("news",                 f"reddit_fantasybaseball_articles_{DATE}.json")
FILE_BOX      = path("boxscores",            f"mlb_boxscores_{YDAY}.json")
OUT_FILE      = f"structured_players_{DATE}.json"

UPLOAD_TO_S3 = False
BUCKET, REGION = "fantasy-sports-csvs", "us-east-1"
S3_KEY = f"{BASE}/combined/{OUT_FILE}"

# ───────── helpers ─────────
def load_json(p): 
    return json.load(open(p)) if os.path.exists(p) else []

def normalize(n): return re.sub(r"[ .'-]", "", n).lower()

def safe_get(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

# ───────── load feeds ─────────
rosters   = load_json(FILE_ROSTER)    # required
starters  = load_json(FILE_STARTERS)
weather   = load_json(FILE_WEATHER)
odds      = load_json(FILE_ODDS)
espn      = load_json(FILE_ESPN)
reddit    = load_json(FILE_REDDIT)
boxscores = load_json(FILE_BOX)

if not rosters:
    sys.exit("❌ roster JSON missing – abort")

# ───────── index helpers ─────────
weather_by_team = {w["team"]: w["weather"] for w in weather}

box_by_pid = {}
for b in boxscores:
    pid = safe_get(b, "player_id", "id", "mlb_id")
    if pid: box_by_pid[str(pid)] = b

starter_names = {normalize(safe_get(g, "home_pitcher", default="")) for g in starters} | \
                {normalize(safe_get(g, "away_pitcher", default="")) for g in starters}

team_to_gamepk, team_to_opp = {}, {}
for g in starters:
    gp   = safe_get(g, "game_pk")
    home = safe_get(g, "home_team_id")
    away = safe_get(g, "away_team_id")
    if gp and home and away:
        team_to_gamepk[home] = team_to_gamepk[away] = gp
        team_to_opp[home] = away
        team_to_opp[away] = home

bet_by_team = {}
for o in odds:
    tid = safe_get(o, "team_id", "teamId", "team")
    if tid: bet_by_team[tid] = o

def surname(r):
    return safe_get(r, "last_name", "surname") or r.get("name", "").split()[-1]

espn_cnt, reddit_cnt = Counter(), Counter()
for art in espn:
    title = str(art.get("headline", "")).lower()
    for r in rosters:
        ln = surname(r)
        if ln and ln.lower() in title:
            espn_cnt[r["player_id"]] += 1
for post in reddit:
    txt = str(post.get("title", "")).lower()
    for r in rosters:
        ln = surname(r)
        if ln and ln.lower() in txt:
            reddit_cnt[r["player_id"]] += 1

# ───────── build players ─────────
players_out = {}
for r in rosters:
    pid = str(r["player_id"])
    tid = safe_get(r, "team_id", "teamId")
    team = r["team"]
    if tid is None:       # no team context → skip
        continue

    name = r.get("name") or f"{r.get('first_name','')} {surname(r)}".strip()
    players_out[name] = {
        "player_id": pid,
        "name": name,
        "team": team,
        "team_id": tid,
        "position": r["position"],
        "handedness": {"bats": r["bats"], "throws": r["throws"]},
        "roster_status": {
            "status_code":        r["status_code"],
            "status_description": r["status_description"],
        },
        "starter": normalize(name) in starter_names if r["position"] == "P" else False,
        "opponent_team_id": team_to_opp.get(tid),
        "game_pk": team_to_gamepk.get(tid),
        "weather_context": weather_by_team.get(team),
        "betting_context": bet_by_team.get(tid),
        "espn_mentions":   espn_cnt.get(r["player_id"], 0),
        "reddit_mentions": reddit_cnt.get(r["player_id"], 0),
        "box_score":       box_by_pid.get(pid, {}),
    }

print(f"✅ built {len(players_out)} player rows")

json.dump(players_out, open(OUT_FILE, "w"), indent=2)
print(f"💾 wrote {OUT_FILE}")

if UPLOAD_TO_S3:
    boto3.client("s3", region_name=REGION).upload_file(OUT_FILE, BUCKET, S3_KEY)
    print(f"☁️ uploaded to s3://{BUCKET}/{S3_KEY}")
