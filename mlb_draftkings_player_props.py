import requests
import json
import pandas as pd
from datetime import datetime
import boto3
import os

# Get today's date
today = datetime.today().strftime('%Y-%m-%d')
filename = f"mlb_draftkings_props_{today}.csv"
local_path = f"/mnt/data/{filename}"

# DraftKings API endpoint
url = "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/84240/categories/boosts?format=json"  # 84240 = MLB

# Props endpoint (baseball game lines with player props)
props_url = "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/84240?category=player-props&format=json"

# Fetch data
response = requests.get(props_url)
data = response.json()

# Extract player props markets
markets = data.get("eventGroup", {}).get("offerCategories", [])
rows = []

for category in markets:
    for subcat in category.get("offerSubcategoryDescriptors", []):
        for offer in subcat.get("offerSubcategory", {}).get("offers", []):
            for outcome in offer:
                try:
                    player = outcome["label"]
                    prop_type = subcat["name"]
                    team = outcome["participant"]
                    line = outcome.get("line", None)
                    odds = outcome.get("oddsAmerican", None)
                    rows.append({
                        "Date": today,
                        "Player": player,
                        "Prop Type": prop_type,
                        "Team": team,
                        "Line": line,
                        "Odds": odds
                    })
                except:
                    continue

# Save to CSV
df = pd.DataFrame(rows)
df.to_csv(local_path, index=False)

# Upload to S3
s3 = boto3.client('s3')
bucket_name = "fantasy-sports-csvs"
s3_path = f"baseball/props/{filename}"

s3.upload_file(local_path, bucket_name, s3_path)
print(f"✅ Uploaded to S3: {s3_path}")
