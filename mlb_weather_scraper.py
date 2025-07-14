import os
import csv
import requests
import pandas as pd
from datetime import datetime
import boto3

# === CONFIG ===
BASE_URL = "https://api.open-meteo.com/v1/forecast"
INPUT_CSV = "mlb_stadium_coordinates.csv"
REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/weather"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_weather_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === READ STADIUM COORDINATES & DOME STATUS ===
df_coords = pd.read_csv(INPUT_CSV)
df_coords["Stadium"] = df_coords["Stadium"].str.lower()
rows = []

print(f"📡 Requesting weather data for {len(df_coords)} stadiums...")

for _, row in df_coords.iterrows():
    team = row["Team"]
    stadium = row["Stadium"]
    lat = row["Latitude"]
    lon = row["Longitude"]
    is_dome = str(row.get("Is_Dome", "FALSE")).strip().lower() == "true"

    try:
        response = requests.get(BASE_URL, params={
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,precipitation_probability,cloudcover,windspeed_10m,winddirection_10m,weathercode",
            "timezone": "auto"
        })

        if response.status_code != 200:
            print(f"❌ Failed to get weather for {team}: {response.text}")
            continue

        data = response.json()
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        if not times:
            print(f"⚠️ No hourly data found for {team}")
            continue

        # Take the first time block (usually current or next hour)
        first_index = 0
        rows.append({
            "date": DATE,
            "team": team,
            "stadium": stadium.title(),  # Title case for display
            "temperature": hourly["temperature_2m"][first_index],
            "windSpeed": hourly["windspeed_10m"][first_index],
            "windDirection": hourly["winddirection_10m"][first_index],
            "precipitationProbability": hourly["precipitation_probability"][first_index],
            "cloudCover": hourly["cloudcover"][first_index],
            "condition": hourly["weathercode"][first_index],
            "time": times[first_index],
            "weatherImpact": "No (Dome)" if is_dome else "Yes"
        })

    except Exception as e:
        print(f"❌ Error processing {team}: {e}")

print(f"✅ Collected weather data for {len(rows)} stadiums")

# === SAVE LOCALLY ===
csv_file = FILENAME
with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"💾 Temp file written locally: {csv_file}")

# === UPLOAD TO S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(csv_file, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === CLEANUP ===
os.remove(csv_file)
print(f"🧹 Cleaned up local file {csv_file}")
