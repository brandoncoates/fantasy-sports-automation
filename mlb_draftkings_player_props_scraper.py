import os
import csv
import json
import time
import boto3
import requests
from datetime import datetime

# === CONFIG ===
REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/playerprops"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_draftkings_player_props_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === DRAFTKINGS URL ===
URL = "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/84240?category=player-props&format=json"

# === FETCH DATA ===
print("📡 Requesting DraftKings player props...")
response = requests.get(URL)
if response.status_code != 200:
    print(f"❌ API error {response.status_code}: {response.text}")
    exit(1)

data = response.json()
events = {e['eventId']: e for e in data['eventGroup']['events']}
props = []

# === PROCESS PROPS ===
for offer_cat in data['eventGroup']['offerCategories']:
    if offer_cat.get('name') != 'Player Props':
        continue
    for subcat in offer_cat.get('offerSubcategoryDescriptors', []):
        prop_type = subcat.get('name')  # Example: "Hits", "Home Runs"
        for offer in subcat.get('offers', []):
            for market in offer:
                event_id = market.get('eventId')
                if not event_id or event_id not in events:
                    continue
                event = events[event_id]
                game = f"{event['teamA']} vs {event['teamB']}"
                commence = event['startDate']
                for outcome in market.get('outcomes', []):
                    props.append({
                        "date": DATE,
                        "time": commence,
                        "game": game,
                        "prop_type": prop_type,
                        "player": outcome.get("participant"),
                        "line": outcome.get("line"),
                        "odds": outcome.get("oddsDecimal"),
                        "side": outcome.get("label"),
                    })

print(f"✅ Collected {len(props)} player props")

# === SAVE LOCALLY ===
csv_file = FILENAME
with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=props[0].keys())
    writer.writeheader()
    writer.writerows(props)
print(f"💾 Local CSV saved: {csv_file}")

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
print(f"🧹 Cleaned up local file: {csv_file}")
