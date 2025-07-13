import os
import csv
import requests
import pandas as pd
from datetime import datetime

# === CONFIG ===
API_KEY = os.environ.get("TOMORROW_IO_API_KEY")
BASE_URL = "https://api.tomorrow.io/v4/weather/forecast"
INPUT_CSV = "mlb_stadium_coordinates.csv"
REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/weather"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_weather_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === READ STADIUM COORDINATES ===
df_coords = pd.read_csv(INPUT_CSV)
rows = []

print(f"📡 Requesting weather data for {len(df_coords)} stadiums...")

for _, row in df_coords.iterrows():
    team = row["Team"]
    stadium = row["Stadium"]
    lat = row["Latitude"]
    lon = row["Longitude"]

    try:
        response = requests.get(BASE_URL, params={
            "location": f"{lat},{lon}",
            "apikey": API_KEY,
            "timesteps": "1h",
            "units": "imperial"
        })

        if response.status_code != 200:
            print(f"❌ Failed to get weather for {team}: {response.text}")
            continue

        data = response.json()
        hourly = data.get("timelines", {}).get("hourly", [])
        if not hourly:
            print(f"⚠️ No hourly data found for {team}")
            continue

        # Grab the next available hour
        weather = hourly[0]
        values = weather.get("values", {})

        rows.append({
            "date": DATE,
            "team": team,
            "stadium": stadium,
            "temperature": values.get("temperature"),
            "windSpeed": values.get("windSpeed"),
            "windDirection": values.get("windDirection"),
            "precipitationProbability": values.get("precipitationProbability"),
            "cloudCover": values.get("cloudCover"),
            "condition": values.get("weatherCode"),
            "time": weather.get("time")
        })

    except Exception as e:
        print(f"❌ Error processing {team}: {e}")

print(f"✅ Collected weather data for {len(rows)} stadiums")

# === SAVE LOCALLY (TEMPORARY FOR DEBUGGING) ===
csv_file = FILENAME
with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"💾 Temp file written locally: {csv_file}")

# === UPLOAD TO S3 ===
import boto3

print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(csv_file, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === CLEANUP TEMP FILE ===
os.remove(csv_file)
print(f"🧹 Cleaned up local file {csv_file}")
