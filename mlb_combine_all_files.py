#!/usr/bin/env python3
import os, re, json, sys, boto3
from datetime import datetime, timedelta
from collections import Counter

# ───── DATE CONFIG ─────
DATE = os.getenv("FORCE_DATE", datetime.now().strftime("%Y-%m-%d"))
YDAY = (datetime.strptime(DATE, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
BASE = os.getenv("COMBINE_BASE_DIR", "baseball").strip("/")

def path(folder, fn): return f"{BASE}/{folder}/{fn}"

FILE_ROSTER   = path("rosters",          f"mlb_rosters_{DATE}.json")
FILE_STARTERS = path("probablestarters", f"mlb_probable_starters_{DATE}.json")
FILE_WEATHER  = path("weather",          f"mlb_weather_{DATE}.json")
FILE_ODDS     = path("betting",          f"mlb_betting_odds_{DATE}.json")
FILE_ESPN     = path("news",             f"mlb_espn_articles_{DATE}.json")
FILE_REDDIT   = path("news",             f"reddit_fantasybaseball_articles_{DATE}.json")
FILE_BOX      = path("boxscores",        f"mlb_boxscores_{YDAY}.json")

OUT_FILE      = f"structured_players_{DATE}.json"

# ───── S3 CONFIG ─────
UPLOAD_TO_S3 = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"
BUCKET       = "fantasy-sports-csvs"
REGION       = "us-east-1"
S3_KEY       = f"{BASE}/combined/{OUT_FILE}"

# ───── HELPERS ─────
def load(path):
    if not os.path.exists(path):
        print(f"⚠️  File missing: {path}")
        return []
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            print(f"❌ JSON decode error in: {path}")
            return []

def normalize(n): return re.sub(r"[ .'-]", "", n).lower()

def safe_get(d: dict, *keys, default=None):
    for k in keys:
        v = d.get(k)
        if v not in (None, "", []):
            return v
    return default

def roster_surname(row):
    ln = safe_get(row, "last_name", "surname")
    if ln: return ln
    full = row.get("name", "").strip()
    return full.split()[-1] if full else None

# ───── LOAD DATA ─────
rosters   = load(FILE_ROSTER)
starters  = load(FILE_STARTERS)
weather   = load(FILE_WEATHER)
odds      = load(FILE_ODDS)
espn      = load(FILE_ESPN)
reddit    = load(FILE_REDDIT)
boxscores = load(FILE_BOX)

if not rosters:
    print("❌ No rosters loaded. Aborting.")
    sys.exit(1)

print(f"🔢 Loaded: {len(rosters)} rosters | {len(starters)} starters | {len(boxscores)} boxscores")

# ───── INDEXES ─────
weather_by_team = {w["team"]: w["weather"] for w in weather}

box_by_pid = {str(pid): b for b in boxscores
              if (pid := safe_get(b, "player_id", "id", "mlb_id"))}

starter_names = {normalize(safe_get(g, "home_pitcher", default="")) for g in starters} | \
                {normalize(safe_get(g, "away_pitcher", default="")) for g in starters}

team_to_gamepk, team_to_opp = {}, {}
for g in starters:
    gp, home, away = g.get("game_pk"), g.get("home_team_id"), g.get("away_team_id")
    if gp and home and away:
        team_to_gamepk[home] = team_to_gamepk[away] = gp
        team_to_opp[home], team_to_opp[away] = away, home

bet_by_team = {tid: o for o in odds if (tid := safe_get(o, "team_id", "teamId", "team"))}

# ───── MENTIONS ─────
espn_cnt, reddit_cnt = Counter(), Counter()
for art in espn:
    title = str(art.get("headline", "")).lower()
    for r in rosters:
        ln = roster_surname(r)
        if ln and ln.lower() in title:
            espn_cnt[r["player_id"]] += 1
for post in reddit:
    txt = str(post.get("title", "")).lower()
    for r in rosters:
        ln = roster_surname(r)
        if ln and ln.lower() in txt:
            reddit_cnt[r["player_id"]] += 1

# ───── BUILD OUTPUT ─────
players = {}
for r in rosters:
    pid = str(r["player_id"])
    tid = safe_get(r, "team_id", "teamId")
    if not tid:
        continue

    ln   = roster_surname(r) or ""
    fn   = r.get("first_name", "")
    name = (fn + " " + ln).strip() or r.get("name", f"Player_{pid}")
    team = r["team"]

    players[name] = {
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

print(f"✅ Built structured entries for {len(players)} players.")

# ───── WRITE FILE ─────
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(players, f, indent=2)
print(f"💾 Wrote file: {OUT_FILE}")

# ───── S3 PUSH ─────
if UPLOAD_TO_S3:
    try:
        boto3.client("s3", region_name=REGION).upload_file(OUT_FILE, BUCKET, S3_KEY)
        print(f"☁️ Uploaded to S3: s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"❌ S3 upload failed: {e}")
