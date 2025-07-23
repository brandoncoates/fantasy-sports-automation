import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
import boto3

# === CONFIG ===
BASE_URL   = "https://api.open-meteo.com/v1/forecast"
INPUT_CSV  = "mlb_stadium_coordinates.csv"
REGION     = "us-east-1"
BUCKET     = "fantasy-sports-csvs"
S3_FOLDER  = "baseball/weather"
DATE       = datetime.now().strftime("%Y-%m-%d")
filename   = f"mlb_weather_{DATE}.json"
S3_KEY     = f"{S3_FOLDER}/{filename}"

# === READ STADIUM COORDINATES & DOME STATUS ===
df_coords = pd.read_csv(INPUT_CSV)
df_coords["Stadium"] = df_coords["Stadium"].str.lower()

records = []
print(f"📡 Requesting weather data for {len(df_coords)} stadiums on {DATE}...")

for _, row in df_coords.iterrows():
    team      = row["Team"]
    stadium   = row["Stadium"].title()
    lat       = row["Latitude"]
    lon       = row["Longitude"]
    is_dome   = str(row.get("Is_Dome", "FALSE")).strip().lower() == "true"

    try:
        resp = requests.get(BASE_URL, params={
            "latitude":               lat,
            "longitude":              lon,
            "hourly":                 "temperature_2m,precipitation_probability,cloudcover,windspeed_10m,winddirection_10m,weathercode",
            "timezone":               "auto"
        })
        resp.raise_for_status()
        data   = resp.json().get("hourly", {})
        times  = data.get("time", [])

        if not times:
            print(f"⚠️ No hourly data for {team}")
            continue

        idx = 0  # first available hour
        records.append({
            "date":                     DATE,
            "team":                     team,
            "stadium":                  stadium,
            "time":                     times[idx],
            "temperature":              data["temperature_2m"][idx],
            "windSpeed":                data["windspeed_10m"][idx],
            "windDirection":            data["winddirection_10m"][idx],
            "precipitationProbability": data["precipitation_probability"][idx],
            "cloudCover":               data["cloudcover"][idx],
            "condition":                data["weathercode"][idx],
            "weatherImpact":            "No (Dome)" if is_dome else "Yes"
        })

    except Exception as e:
        print(f"❌ Error for {team}: {e}")

print(f"✅ Collected weather for {len(records)} stadiums")

# === SAVE TO JSON LOCALLY ===
os.makedirs("mlb_weather", exist_ok=True)
local_path = os.path.join("mlb_weather", filename)
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)
print(f"💾 JSON written locally: {local_path}")

# === UPLOAD TO S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(local_path, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload to S3 failed: {e}")
    exit(1)

# === CLEANUP ===
os.remove(local_path)
print(f"🧹 Cleaned up local file {local_path}")
