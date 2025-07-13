import os
import csv
import boto3
import requests
from datetime import datetime

# === CONFIG ===
API_KEY = os.environ.get("ODDS_API_KEY")
REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/playerprops"
SPORT = "baseball_mlb"
REGIONS = "us"

# ✅ Use a valid market (player_props is NOT valid)
MARKETS = "player_home_runs"  # Can later be: "player_hits,player_home_runs"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_oddsapi_player_props_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === BUILD URL ===
url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
params = {
    "apiKey": API_KEY,
    "regions": REGIONS,
    "markets": MARKETS,
    "oddsFormat": "decimal"
}

print(f"📡 Requesting player props from Odds API with market(s): {MARKETS}")
response = requests.get(url, params=params)
if response.status_code != 200:
    print(f"❌ API error {response.status_code}: {response.text}")
    exit(1)

data = response.json()
print(f"📊 Received {len(data)} player prop events")

# === PROCESS DATA ===
rows = []
for event in data:
    game = event.get("home_team", "N/A") + " vs " + event.get("away_team", "N/A")
    commence_time = event.get("commence_time")
    for bookmaker in event.get("bookmakers", []):
        book_name = bookmaker.get("title")
        for market in bookmaker.get("markets", []):
            market_key = market.get("key")
            for outcome in market.get("outcomes", []):
                rows.append({
                    "date": DATE,
                    "game": game,
                    "player": outcome.get("name"),
                    "prop": market_key,
                    "book": book_name,
                    "value": outcome.get("point"),
                    "odds": outcome.get("price"),
                    "time": commence_time
                })

if not rows:
    print("⚠️ No rows collected. Possibly empty props for selected market.")
    exit(0)

