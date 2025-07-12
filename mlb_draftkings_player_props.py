import os
import requests
import pandas as pd
from datetime import datetime

# === CONFIG ===
output_dir = "mlb_draftkings_player_props"
os.makedirs(output_dir, exist_ok=True)
date_str = datetime.now().strftime('%Y-%m-%d')
filename = f"mlb_draftkings_player_props_{date_str}.csv"
output_path = os.path.join(output_dir, filename)

# DraftKings API endpoint for MLB player props
props_url = "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/84240?category=player-props&format=json"

# Fetch data
response = requests.get(props_url)
if response.status_code != 200:
    raise Exception(f"❌ Failed to fetch DraftKings player props: Status {response.status_code}")

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
                    team = outcome.get("participant", "")
                    line = outcome.get("line")
                    odds = outcome.get("oddsAmerican")

                    rows.append({
                        "Date": date_str,
                        "Player": player,
                        "Prop Type": prop_type,
                        "Team": team,
                        "Line": line,
                        "Odds": odds
                    })
                except Exception as e:
                    print(f"⚠️ Skipped malformed outcome: {e}")
                    continue

# Save to CSV
df = pd.DataFrame(rows)
df.to_csv(output_path, index=False)

print(f"✅ Saved {len(df)} player props to {output_path}")
