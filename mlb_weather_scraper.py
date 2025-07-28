#!/usr/bin/env python3
"""
Collect per‑stadium weather for today’s MLB slate (including open‑air parks
and special‑case Athletics in Sacramento) and upload to S3. Falls back to
using the hourly forecast, then the `current_weather` snapshot if hourly
is empty, and finally stubs out any teams that failed entirely. Adds a
1‑second delay and debug logging to improve reliability.
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
import boto3

# ───── CONFIG ─────
BASE_URL   = "https://api.open-meteo.com/v1/forecast"
INPUT_CSV  = "mlb_stadium_coordinates.csv"  # replace with your updated CSV name
REGION     = "us-east-2"
BUCKET     = "fantasy-sports-csvs"
S3_FOLDER  = "baseball/weather"

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
    # Special‑case Athletics in Sacramento
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
        "latitude":  lat,
        "longitude": lon,
        "hourly": (
            "temperature_2m,relativehumidity_2m,"
            "windspeed_10m,winddirection_10m,"
            "precipitation_probability,cloudcover,weathercode"
        ),
        "current_weather": True,
        "timezone": "auto",
    }

    try:
        resp = requests.get(BASE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data   = resp.json()
        hourly = data.get("hourly", {})
        times  = hourly.get("time", [])

        # debug: how many hourly points did we get?
        print(f"🔍 {team}: coords={lat:.4f},{lon:.4f} → hourly_count={len(times)}")

        if times:
            # use the first hour of forecast
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
            # fallback to current_weather snapshot
            cw = data.get("current_weather", {})
            print(f"⏱️  Falling back to current_weather for {team}")
            temp_f   = round(cw.get("temperature", 0) * 9/5 + 32, 1) if cw.get("temperature") is not None else None
            wind_mph = round(cw.get("windspeed", 0) * 0.621371, 1) if cw.get("windspeed") is not None else None
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
            "date": DATE,
            "team": team,
            "stadium": stadium,
            "time_local": time_local,
            "weather": weather_block,
            "precipitation_probability": precip,
            "cloud_cover_pct":           cloud,
            "weather_code":              code,
        })

    except Exception as e:
        print(f"⚠️  Could not fetch {team} ({stadium}): {e}")

    # pause to avoid rate‑limits
    time.sleep(1)

# ───── STUB‑FALLBACK FOR ANY REMAINING MISSING STADIUMS ─────
fetched = {r["team"] for r in records}
stubbed = 0
for _, row in df.iterrows():
    team = row["Team"]
    if team in fetched:
        continue

    if team == "Oakland Athletics":
        stadium = "Sutter Health Park"
        is_dome = False
    else:
        stadium = row["Stadium"].title()
        is_dome = str(row.get("Is_Dome","")).strip().lower() == "true"

    print(f"🔨 Stubbing missing: {team} / {stadium}")
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
try:
    s3.upload_file(local_path, BUCKET, S3_KEY)
    print(f"✅ Uploaded to s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ S3 upload failed: {e}")
    exit(1)

os.remove(local_path)
print(f"🧹 Cleaned up {local_path}")
