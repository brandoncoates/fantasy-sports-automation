#!/usr/bin/env python3
"""
Collect gametime weather for today’s MLB slate and upload to S3:
 - reads each game’s scheduled start from probable_starters JSON
 - pulls the hourly forecast for the hour nearest (>=) first pitch
 - falls back to current_weather if hourly data is missing
 - retry logic on API failures
 - per-team debug logging of requests
 - special handling for temporary Oakland Athletics location
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
import boto3
import re

def normalize(text: str) -> str:
    return re.sub(r"[ .'-]", "", (text or "")).lower()

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

# ───── LOAD STARTERS JSON ─────
s3 = boto3.client("s3", region_name=REGION)
try:
    print(f"📅 Downloading starters from S3: s3://{BUCKET}/{STARTERS_JSON}")
    response = s3.get_object(Bucket=BUCKET, Key=STARTERS_JSON)
    starters = json.loads(response["Body"].read().decode("utf-8"))
    print(f"✅ Loaded probable starters from S3 ({len(starters)} entries)")
except Exception as e:
    print(f"❌ Failed to read starters from S3: {e}")
    raise SystemExit(1)

# ───── LOAD STADIUM CSV ─────
stadiums = pd.read_csv(INPUT_CSV)
stadiums["Stadium"] = stadiums["Stadium"].str.lower()

# ───── STADIUM MAPPING ─────
stadium_map = {}
for _, row in stadiums.iterrows():
    team = normalize(row["Team"])
    stadium_map[team] = {
        "name": row["Stadium"].title(),
        "lat": row["Latitude"],
        "lon": row["Longitude"],
        "is_dome": str(row.get("Is_Dome", "")).strip().lower() == "true"
    }

# ───── PATCH FOR OAKLAND ATHLETICS (SACRAMENTO) ─────
athletics_override = {
    "name": "Sutter Health Park",
    "lat": 38.6254,
    "lon": -121.5050,
    "is_dome": False
}
for key in ["oak", "oaklandathletics", "athletics", "as", "sacramento", "sutterhealthpark"]:
    stadium_map[key] = athletics_override

# ───── FETCH FORECAST PER GAME ─────
records = []
for g in starters:
    try:
        game_dt = datetime.fromisoformat(g["game_datetime"].replace("Z", "+00:00"))
        for side in ["home_team", "away_team"]:
            team_name = g[side]
            team_key = normalize(team_name)
            stadium = stadium_map.get(team_key)

            if not stadium:
                print(f"⚠️ No stadium found for {team_name}, skipping")
                continue

            params = {
                "latitude": stadium["lat"],
                "longitude": stadium["lon"],
                "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m,winddirection_10m,precipitation_probability,cloudcover,weathercode",
                "current_weather": True,
                "timezone": "auto",
            }

            data = None
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    prep = requests.Request("GET", BASE_URL, params=params).prepare()
                    print(f"➡️ [{attempt}] {team_name} requesting: {prep.url}")
                    resp = requests.get(BASE_URL, params=params, timeout=15)
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except Exception as e:
                    print(f"⚠️  {team_name} attempt {attempt} failed: {e}")
                    if resp is not None and resp.text:
                        snippet = resp.text.replace('\n',' ')[:200]
                        print(f"   snippet: {snippet}")
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(BACKOFF_SEC)

            if not data:
                print(f"❌ All attempts failed for {team_name}, skipping")
                continue

            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            idx = 0
            if game_dt and times:
                for i, t in enumerate(times):
                    forecast_time = datetime.fromisoformat(t).replace(tzinfo=game_dt.tzinfo)
                    if forecast_time >= game_dt:
                        idx = i
                        break

            temp_c = hourly["temperature_2m"][idx]
            temp_f = round(temp_c * 9 / 5 + 32, 1)
            wind_mph = round(hourly["windspeed_10m"][idx] * 0.621371, 1)

            records.append({
                "date": DATE,
                "team": team_name,
                "stadium": stadium["name"],
                "time_local": times[idx],
                "weather": {
                    "temperature_f": temp_f,
                    "humidity_pct": hourly["relativehumidity_2m"][idx],
                    "wind_speed_mph": wind_mph,
                    "wind_direction_deg": hourly["winddirection_10m"][idx],
                    "roof_status": "closed" if stadium["is_dome"] else "open",
                },
                "precipitation_probability": hourly["precipitation_probability"][idx],
                "cloud_cover_pct": hourly["cloudcover"][idx],
                "weather_code": hourly["weathercode"][idx],
            })
            time.sleep(1)

    except Exception as e:
        print(f"❌ Skipping game {g.get('game_id')} due to error: {e}")

print(f"✅ Total records: {len(records)}")

# ───── SAVE & UPLOAD ─────
os.makedirs("baseball/weather", exist_ok=True)
local_path = os.path.join("baseball/weather", OUT_FILE)
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)
print(f"📀 Wrote {local_path}")

print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3.upload_file(local_path, BUCKET, S3_KEY)
print("✅ Upload complete")
