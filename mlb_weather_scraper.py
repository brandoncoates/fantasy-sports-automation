#!/usr/bin/env python3
"""
Collect per‑stadium weather for today’s MLB slate and upload to S3, with:
 - hourly forecast
 - fallback to current_weather
 - retry logic on API failures
 - per‑team debug logging
 - stub fallback only after all retries and fallbacks fail
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
import boto3

# ───── CONFIG ─────
BASE_URL       = "https://api.open-meteo.com/v1/forecast"
INPUT_CSV      = "mlb_stadium_coordinates.csv"        # your updated CSV
REGION         = "us-east-2"
BUCKET         = "fantasy-sports-csvs"
S3_FOLDER      = "baseball/weather"
MAX_ATTEMPTS   = 3
BACKOFF_SEC    = 2

DATE      = datetime.now().strftime("%Y-%m-%d")
FILENAME  = f"mlb_weather_{DATE}.json"
S3_KEY    = f"{S3_FOLDER}/{FILENAME}"

# ───── READ STADIUM COORDINATES ─────
df = pd.read_csv(INPUT_CSV)
df["Stadium"] = df["Stadium"].str.lower()

records = []
print(f"📡 Fetching weather for {len(df)} stadiums on {DATE}…")

for _, row in df.iterrows():
    team = row["Team"]
    # Athletics in Sacramento override
    if team == "Oakland Athletics":
        stadium = "Sutter Health Park"
        lat, lon = 38.6254, -121.5050
        is_dome = False
    else:
        stadium = row["Stadium"].title()
        lat     = row["Latitude"]
        lon     = row["Longitude"]
        is_dome = str(row.get("Is_Dome","")).strip().lower() == "true"

    params = {
        "latitude":       lat,
        "longitude":      lon,
        "hourly": (
            "temperature_2m,relativehumidity_2m,"
            "windspeed_10m,winddirection_10m,"
            "precipitation_probability,cloudcover,weathercode"
        ),
        "current_weather": True,
        "timezone":        "auto",
    }

    data = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            print(f"⚠️  {team} attempt {attempt}/{MAX_ATTEMPTS} failed: {e}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF_SEC)
    if data is None:
        print(f"❌ All attempts failed for {team}; will stub later")
        continue

    hourly = data.get("hourly", {})
    times  = hourly.get("time", [])
    print(f"🔍 {team}: coords={lat:.4f},{lon:.4f} → hourly_count={len(times)}")

    if times:
        idx = 0
        temp_c   = hourly["temperature_2m"][idx]
        temp_f   = round(temp_c * 9/5 + 32, 1)
        wind_mph = round(hourly["windspeed_10m"][idx] * 0.621371, 1)
        weather_block = {
            "temperature_f":      temp_f,
            "humidity_pct":       hourly["relativehumidity_2m"][idx],
            "wind_speed_mph":     wind_mph,
            "wind_direction_deg": hourly["winddirection_10m"][idx],
            "roof_status":        "closed" if is_dome else "open",
        }
        time_local = times[idx]
        precip     = hourly["precipitation_probability"][idx]
        cloud      = hourly["cloudcover"][idx]
        code       = hourly["weathercode"][idx]
    else:
        cw = data.get("current_weather", {})
        print(f"⏱️  Falling back to current_weather for {team}")
        temp_f   = round(cw.get("temperature",0) * 9/5 + 32, 1) if cw.get("temperature") is not None else None
        wind_mph = round(cw.get("windspeed",0) * 0.621371, 1) if cw.get("windspeed") is not None else None
        weather_block = {
            "temperature_f":      temp_f,
            "humidity_pct":       None,
            "wind_speed_mph":     wind_mph,
            "wind_direction_deg": cw.get("winddirection"),
            "roof_status":        "closed" if is_dome else "open",
        }
        time_local = cw.get("time")
        precip     = None
        cloud      = None
        code       = cw.get("weathercode")

    records.append({
        "date":                      DATE,
        "team":                      team,
        "stadium":                   stadium,
        "time_local":                time_local,
        "weather":                   weather_block,
        "precipitation_probability": precip,
        "cloud_cover_pct":           cloud,
        "weather_code":              code,
    })

    time.sleep(1)

# ───── STUB‑FALLBACK FOR REMAINING MISSING ─────
fetched = {r["team"] for r in records}
stubbed = 0
for _, row in df.iterrows():
    team = row["Team"]
    if team in fetched:
        continue
    stadium = "Sutter Health Park" if team == "Oakland Athletics" else row["Stadium"].title()
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
os.makedirs("mlb_weather", exist_ok=True)
local_path = os.path.join("mlb_weather", FILENAME)
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)
print(f"💾 Wrote local JSON: {local_path}")

s3 = boto3.client("s3", region_name=REGION)
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3.upload_file(local_path, BUCKET, S3_KEY)
print(f"✅ Uploaded to s3://{BUCKET}/{S3_KEY}")

os.remove(local_path)
print(f"🧹 Cleaned up {local_path}")
