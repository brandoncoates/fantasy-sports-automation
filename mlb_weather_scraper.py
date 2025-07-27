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
INPUT_CSV    = "mlb_stadium_coordinates.csv"         # columns: Team,Stadium,Latitude,Longitude,Is_Dome
REGION       = "us-east-1"
BUCKET       = "fantasy-sports-csvs"
S3_FOLDER    = "baseball/weather"

DATE    = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_weather_{DATE}.json"
S3_KEY   = f"{S3_FOLDER}/{FILENAME}"

# ───── READ STADIUM COORDINATES ─────
df_coords = pd.read_csv(INPUT_CSV)
df_coords["Stadium"] = df_coords["Stadium"].str.lower()

records = []
print(f"📡 Requesting weather data for {len(df_coords)} stadiums on {DATE}…")

for _, row in df_coords.iterrows():
    team    = row["Team"]
    stadium = row["Stadium"].title()
    lat     = row["Latitude"]
    lon     = row["Longitude"]
    is_dome = str(row.get("Is_Dome", "FALSE")).strip().lower() == "true"

    try:
        resp = requests.get(
            BASE_URL,
            params={
                "latitude":  lat,
                "longitude": lon,
                "hourly": (
                    "temperature_2m,relativehumidity_2m,"
                    "windspeed_10m,winddirection_10m,"
                    "precipitation_probability,cloudcover,weathercode"
                ),
                "timezone": "auto",
            },
            timeout=15,
        )
        resp.raise_for_status()
        hourly = resp.json().get("hourly", {})
        times  = hourly.get("time", [])
        if not times:
            raise ValueError("no hourly data")

        idx     = 0  # you can refine to first‑pitch hour if you like
        temp_c  = hourly["temperature_2m"][idx]
        temp_f  = round(temp_c * 9/5 + 32, 1)
        wind_kph = hourly["windspeed_10m"][idx]
        wind_mph = round(wind_kph * 0.621371, 1)

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

    except Exception as e:
        print(f"⚠️ Skipping {team} ({stadium}): {e}")

# ───── STUB OUT ANY MISSING STADIUMS ─────
fetched_teams = {r["team"] for r in records}
stubbed = 0
for _, row in df_coords.iterrows():
    team    = row["Team"]
    if team in fetched_teams:
        continue
    stadium = row["Stadium"].title()
    is_dome = str(row.get("Is_Dome", "FALSE")).strip().lower() == "true"
    stub = {
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
    }
    records.append(stub)
    stubbed += 1

print(f"✅ Collected weather for {len(records)} stadiums (stubbed {stubbed})")

# ───── SAVE LOCALLY ─────
os.makedirs("mlb_weather", exist_ok=True)
local_path = os.path.join("mlb_weather", FILENAME)
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)
print(f"💾 JSON written locally: {local_path}")

# ───── UPLOAD TO S3 ─────
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(local_path, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload to S3 failed: {e}")
    exit(1)

# ───── CLEANUP ─────
os.remove(local_path)
print(f"🧹 Cleaned up local file {local_path}")
