#!/usr/bin/env python3
"""
Collect gametime weather for today’s MLB slate and upload to S3:
 - reads each game’s scheduled start from probable_starters JSON
 - pulls the hourly forecast for the hour nearest (>=) first pitch
 - falls back to current_weather if hourly data is missing
 - retry logic on API failures
 - per‑team debug logging of requests
 - final stub fallback only if all else fails
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
import boto3

# ───── HELPERS ─────
import re
def normalize(text: str) -> str:
    return re.sub(r"[ .'-]", "", (text or "")).lower()

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
        normalize(club["city"]), normalize(club["nick"]),
        normalize(f"{club['city']} {club['nick']}"),
        normalize(canon), normalize(club["abbr3"]), normalize(club["abbr2"])
    }
    variants |= {v.replace(" ", "") for v in variants}
    for key in variants:
        TEAM_NAME_MAP[key] = canon

# ───── CONFIG ─────
DATE          = datetime.now().strftime("%Y-%m-%d")
BASE_URL      = "https://api.open-meteo.com/v1/forecast"
INPUT_CSV     = "mlb_stadium_coordinates.csv"
STARTERS_JSON = f"baseball/probablestarters/mlb_probable_starters_{DATE}.json"
REGION        = "us-east-2"
BUCKET        = "fantasy-sports-csvs"
S3_FOLDER     = "baseball/weather"
MAX_ATTEMPTS  = 3
BACKOFF_SEC   = 2
OUT_FILE      = f"mlb_weather_{DATE}.json"
S3_KEY        = f"{S3_FOLDER}/{OUT_FILE}"

# ───── LOAD DATA FROM S3 ─────
s3 = boto3.client("s3", region_name=REGION)

try:
    print(f"📥 Downloading starters from S3: s3://{BUCKET}/{STARTERS_JSON}")
    response = s3.get_object(Bucket=BUCKET, Key=STARTERS_JSON)
    content = response["Body"].read().decode("utf-8")
    starters = json.loads(content)
    print(f"✅ Loaded probable starters from S3 ({len(starters)} entries)")
except Exception as e:
    print(f"❌ Failed to read starters from S3: {e}")
    raise SystemExit(1)

df_coords = pd.read_csv(INPUT_CSV)
df_coords["Stadium"] = df_coords["Stadium"].str.lower()

# map each team to its first‑pitch datetime
game_start = {}
for g in starters:
    start_iso = g.get("game_datetime") or f"{g['game_date']}T{g['game_time_local']}"
    dt = datetime.fromisoformat(start_iso)
    home = TEAM_NAME_MAP.get(normalize(g["home_team"]), g["home_team"])
    away = TEAM_NAME_MAP.get(normalize(g["away_team"]), g["away_team"])
    game_start[home] = dt
    game_start[away] = dt

# ───── FETCH FORECAST ─────
records = []
print(f"📡 Fetching gametime weather for {len(df_coords)} teams on {DATE}…")

for _, row in df_coords.iterrows():
    team_raw = row["Team"]
    team     = TEAM_NAME_MAP.get(normalize(team_raw), team_raw)
    # override Athletics
    if team == "Oakland Athletics":
        stadium = "Sutter Health Park"
        lat, lon = 38.6254, -121.5050
        is_dome = False
    else:
        stadium = row["Stadium"].title()
        lat, lon = row["Latitude"], row["Longitude"]
        is_dome = str(row.get("Is_Dome","")).strip().lower() == "true"

    game_dt = game_start.get(team)
    params  = {
        "latitude":       lat,
        "longitude":      lon,
        "hourly":         "temperature_2m,relativehumidity_2m,windspeed_10m,winddirection_10m,precipitation_probability,cloudcover,weathercode",
        "current_weather": True,
        "timezone":        "auto",
    }

    data = None
    for attempt in range(1, MAX_ATTEMPTS+1):
        prep = requests.Request("GET", BASE_URL, params=params).prepare()
        print(f"➡️ [{attempt}] {team} requesting: {prep.url}")
        try:
            resp = requests.get(BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            print(f"⚠️  {team} attempt {attempt}/{MAX_ATTEMPTS} failed: {e}")
            if resp is not None and resp.text:
                snippet = resp.text.replace('\n',' ')[:200]
                print(f"   snippet: {snippet}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF_SEC)

    if not data:
        print(f"❌ All attempts failed for {team}; will stub later")
        continue

    hourly = data.get("hourly", {})
    times  = hourly.get("time", [])
    idx = 0
    if game_dt and times:
        for i, t in enumerate(times):
            if datetime.fromisoformat(t) >= game_dt:
                idx = i
                break

    print(f"🔍 {team}: using idx={idx} for start {game_dt}")

    # extract weather
    temp_c   = hourly["temperature_2m"][idx]
    temp_f   = round(temp_c * 9/5 + 32, 1)
    wind_mph = round(hourly["windspeed_10m"][idx] * 0.621371, 1)
    record = {
        "date": DATE,
        "team": team,
        "stadium": stadium,
        "time_local": times[idx],
        "weather": {
            "temperature_f":      temp_f,
            "humidity_pct":       hourly["relativehumidity_2m"][idx],
            "wind_speed_mph":     wind_mph,
            "wind_direction_deg": hourly["winddirection_10m"][idx],
            "roof_status":        "closed" if is_dome else "open",
        },
        "precipitation_probability": hourly["precipitation_probability"][idx],
        "cloud_cover_pct":           hourly["cloudcover"][idx],
        "weather_code":              hourly["weathercode"][idx],
    }
    records.append(record)
    time.sleep(1)

# ───── STUB‑FALLBACK ─────
fetched = {r["team"] for r in records}
stubbed = 0
for _, row in df_coords.iterrows():
    team = TEAM_NAME_MAP.get(normalize(row["Team"]), row["Team"])
    if team in fetched:
        continue
    stadium = "Sutter Health Park" if team=="Oakland Athletics" else row["Stadium"].title()
    is_dome = str(row.get("Is_Dome","")).strip().lower() == "true"
    print(f"🔨 Stubbing missing: {team}")
    records.append({
        "date": DATE,
        "team": team,
        "stadium": stadium,
        "time_local": None,
        "weather": {
            "temperature_f":      None,
            "humidity_pct":       None,
            "wind_speed_mph":     None,
            "wind_direction_deg": None,
            "roof_status":        "closed" if is_dome else "open",
        },
        "precipitation_probability": None,
        "cloud_cover_pct":           None,
        "weather_code":              None,
    })
    stubbed += 1

print(f"✅ Total records: {len(records)} (stubbed {stubbed})")

# ───── SAVE & UPLOAD ─────
os.makedirs("baseball/weather", exist_ok=True)
local_path = os.path.join("baseball/weather", OUT_FILE)
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)
print(f"💾 Wrote {local_path}")

s3 = boto3.client("s3", region_name=REGION)
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3.upload_file(local_path, BUCKET, S3_KEY)
print("✅ Upload complete")
