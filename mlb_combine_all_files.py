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
ARCHIVE_FILE = "player_game_log.jsonl"

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

# ─── LOAD FILES ───
rosters   = load_json(ROSTER)
starters  = load_json(STARTERS)
weather   = load_json(WEATHER)
odds      = load_json(ODDS)
espn      = load_json(ESPN)
reddit    = load_all_reddit_jsons(DATE)
boxscores = load_json(BOX)

# ─── BUILD TEAM NAME MAP ───
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

for game in starters:
    for k in ["home_team", "away_team"]:
        raw = game.get(k, "")
        TEAM_NAME_MAP[normalize(raw)] = raw
TEAM_NAME_MAP[normalize("As")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("A's")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("Sacramento Athletics")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("Sutter Health Park")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("Athletics")] = "Oakland Athletics"
TEAM_NAME_MAP[normalize("Oakland Athletics")] = "Oakland Athletics"

# ─── WEATHER LOOKUP ───
weather_by_team = {}
weather_grouped = defaultdict(list)
unmatched_weather_teams = set()

for rec in weather:
    raw_team = rec.get("team") or rec.get("team_name") or ""
    norm_team = normalize(raw_team)
    canon = TEAM_NAME_MAP.get(norm_team)

    if canon:
        weather_grouped[canon].append(rec)
    else:
        unmatched_weather_teams.add(raw_team)

if "Athletics" in [rec.get("team") for rec in weather]:
    if "Oakland Athletics" not in weather_grouped and "Athletics" in weather_grouped:
        weather_grouped["Oakland Athletics"] = weather_grouped["Athletics"]

for team, entries in weather_grouped.items():
    canon_team = TEAM_NAME_MAP.get(normalize(team), team)
    weather_by_team[canon_team] = sorted(entries, key=lambda x: x.get("time_local", ""))[0]

if unmatched_weather_teams:
    print(f"⚠️ Unmatched weather teams: {sorted(unmatched_weather_teams)}")
else:
    print("✅ All weather teams matched.")

# ─── MATCHUPS + BETTING ───
bet_by_team = {}
matchup_by_team = {}

for o in odds:
    if o.get("bookmaker") != "FanDuel":
        continue

    h_raw = o.get("home_team", "")
    a_raw = o.get("away_team", "")
    h_team = TEAM_NAME_MAP.get(normalize(h_raw))
    a_team = TEAM_NAME_MAP.get(normalize(a_raw))

    if h_team and a_team:
        matchup_by_team[normalize(h_team)] = {"opponent": a_team, "home_or_away": "home"}
        matchup_by_team[normalize(a_team)] = {"opponent": h_team, "home_or_away": "away"}

        betting_info = {
            "over_under": o.get("over_under"),
            "spread": o.get("spread"),
            "favorite": o.get("favorite"),
            "underdog": o.get("underdog"),
            "implied_totals": o.get("implied_totals", {})
        }

        bet_by_team[h_team] = betting_info
        bet_by_team[a_team] = betting_info

for game in starters:
    h_raw = game.get("home_team", "")
    a_raw = game.get("away_team", "")
    h_team = TEAM_NAME_MAP.get(normalize(h_raw))
    a_team = TEAM_NAME_MAP.get(normalize(a_raw))

    if h_team and normalize(h_team) not in matchup_by_team:
        matchup_by_team[normalize(h_team)] = {"opponent": a_team, "home_or_away": "home"}
    if a_team and normalize(a_team) not in matchup_by_team:
        matchup_by_team[normalize(a_team)] = {"opponent": h_team, "home_or_away": "away"}

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

starter_names = {normalize(game.get("home_pitcher", "")) for game in starters} | {
    normalize(game.get("away_pitcher", "")) for game in starters
}

def extract_game_log_entry(player_name, player_id, team, opponent, box_score, weather, betting, date):
    if not box_score:
        return None

    return {
        "date": date,
        "player_id": player_id,
        "name": player_name,
        "team": team,
        "opponent": opponent,
        "home_or_away": matchup_by_team.get(normalize(team), {}).get("home_or_away"),
        "box_score": box_score,
        "weather": weather.get("weather", {}),
        "betting": betting,
    }

with open(ARCHIVE_FILE, "a", encoding="utf-8") as archive:
    for r in rosters:
        pid = str(r["player_id"])
        name = r["player"].strip()
        raw_team = r.get("team", "")
        club = TEAM_NAME_MAP.get(normalize(raw_team), raw_team)

        club_key = normalize(club)
        canon_club = TEAM_NAME_MAP.get(club_key, club)
        wc = weather_by_team.get(
            canon_club,
            weather_by_team.get(club, {"team": club, "weather": {}, "precipitation_probability": None, "cloud_cover_pct": None, "weather_code": None})
        )

        matchup = matchup_by_team.get(normalize(club), {})
        bet = bet_by_team.get(club, {})

        is_starter = normalize(name) in starter_names

        box = box_by_name.get(normalize(name), {}).copy()
        if r.get("position") not in ["P", "SP", "RP"]:
            for stat in ["Innings Pitched", "Earned Runs", "Strikeouts (Pitching)", "Wins", "Quality Start"]:
                box.pop(stat, None)

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
                "spread": bet.get("spread"),
                "favorite": bet.get("favorite"),
                "underdog": bet.get("underdog"),
                "implied_totals": bet.get("implied_totals", {})
            },
            "espn_mentions": espn_cnt.get(pid, 0),
            "espn_articles": espn_articles_by_pid.get(pid, []),
            "reddit_mentions": reddit_cnt.get(pid, 0),
            "box_score": box,
        }

        # Append to archive if there's a box score
        entry = extract_game_log_entry(name, pid, club, matchup.get("opponent"), box, wc, bet, YDAY)
        if entry:
            archive.write(json.dumps(entry) + "\n")

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
