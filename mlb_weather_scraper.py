#!/usr/bin/env python3
"""
Collect per‑stadium weather for today’s MLB slate (including open‑air parks)
and upload to S3. Falls back to stubbing out any stadiums the API fails to
return, using your Is_Dome flag to set “roof_status”.
"""

import os
import json
import glob
import requests
import pandas as pd
from datetime import datetime
import boto3

# ───── CONFIG ─────
BASE_URL     = "https://api.open-meteo.com/v1/forecast"
INPUT_CSV    = "mlb_stadium_coordinates.csv"  # Team,Stadium,Latitude,Longitude,Is_Dome
REGION       = "us-east-1"
BUCKET       = "fantasy-sports-csvs"
S3_FOLDER    = "baseball/weather"

DATE     = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_weather_{DATE}.json"
S3_KEY   = f"{S3_FOLDER}/{FILENAME}"

# ───── READ STADIUM COORDINATES ─────
df = pd.read_csv(INPUT_CSV)
df["Stadium"] = df["Stadium"].str.lower()

records = []
print(f"📡 Fetching weather for {len(df)} stadiums on {DATE}…")

for _, row in df.iterrows():
    team    = row["Team"]
    stadium = row["Stadium"].title()
    lat     = row["Latitude"]
    lon     = row["Longitude"]
    is_dome = str(row.get("Is_Dome", "")).strip().lower() == "true"

    try:
        resp = requests.get(
            BASE_URL,
            params={
                "latitude":  lat,
                "longitude": lon,
                "hourly": ("temperature_2m,relativehumidity_2m,"
                           "windspeed_10m,winddirection_10m,"
                           "precipitation_probability,cloudcover,weathercode"),
                "timezone": "auto",
            },
            timeout=15,
        )
        resp.raise_for_status()
        hourly = resp.json().get("hourly", {})
        times  = hourly.get("time", [])
        if not times:
            raise ValueError("no hourly data")

        idx      = 0  # you can refine to first‑pitch hour here
        temp_c   = hourly["temperature_2m"][idx]
        temp_f   = round(temp_c * 9/5 + 32, 1)
        wind_kph = hourly["windspeed_10m"][idx]
        wind_mph = round(wind_kph * 0.621371, 1)

        records.append({
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
        })
    except Exception as e:
        print(f"⚠️  Could not fetch {team} ({stadium}): {e}")

# ───── STUB‑FALLBACK FOR MISSING STADIUMS ─────
fetched = {r["team"] for r in records}
stubbed = 0

for _, row in df.iterrows():
    team    = row["Team"]
    if team in fetched:
        continue
    stadium = row["Stadium"].title()
    is_dome = str(row.get("Is_Dome", "")).strip().lower() == "true"

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
print(f"☁️ Uploading s3://{BUCKET}/{S3_KEY}")
try:
    s3.upload_file(local_path, BUCKET, S3_KEY)
    print(f"✅ Uploaded to s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ S3 upload failed: {e}")
    exit(1)

os.remove(local_path)
print(f"🧹 Cleaned up {local_path}")
