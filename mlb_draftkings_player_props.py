import os
import re
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# === CONFIG ===
output_dir = "mlb_draftkings_player_props"
os.makedirs(output_dir, exist_ok=True)
date_str = datetime.now().strftime('%Y-%m-%d')
filename = f"mlb_draftkings_player_props_{date_str}.csv"
output_path = os.path.join(output_dir, filename)

# === Step 1: Get props page from DraftKings
url = 'https://sportsbook.draftkings.com/leagues/baseball/mlb?category=player-props&subcategory=player-hits'
headers = {
    'User-Agent': 'Mozilla/5.0'
}

response = requests.get(url, headers=headers)
if response.status_code != 200:
    raise Exception(f"❌ Failed to fetch DraftKings page: Status {response.status_code}")

soup = BeautifulSoup(response.text, 'html.parser')

# === Step 2: Parse embedded JSON from the page
script_tags = soup.find_all("script")
json_text = ""
for script in script_tags:
    if 'window.__INITIAL_STATE__' in script.text:
        json_text = script.text
        break

if not json_text:
    raise Exception("❌ Could not locate DraftKings JSON payload.")

# Extract JSON string
try:
    match = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.*});', json_text)
    if not match:
        raise Exception("❌ Failed to extract JSON from script tag.")
    json_str = match.group(1)

    import json
    data = json.loads(json_str)
except Exception as e:
    raise Exception(f"❌ JSON parse error: {e}")

# === Step 3: Extract player props from JSON
markets = data.get("markets", {})
player_props = []

for market_id, market_data in markets.items():
    title = market_data.get("label", "")
    if "Hit" not in title:
        continue

    outcomes = market_data.get("outcomes", {})
    for outcome in outcomes.values():
        player = outcome.get("participant", "")
        line = outcome.get("line")
        odds = outcome.get("oddsAmerican")

        if player:
            player_props.append({
                "Date": date_str,
                "Player": player,
                "Prop Type": title,
                "Line": line,
                "Odds": odds
            })

# === Step 4: Save to CSV
df = pd.DataFrame(player_props)
df.to_csv(output_path, index=False)

print(f"✅ Saved {len(df)} player props to {output_path}")
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
