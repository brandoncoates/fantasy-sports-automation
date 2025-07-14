import os
import requests
import pandas as pd
from datetime import datetime

# === CONFIG ===
API_KEY = '32c95ea767253beab2da2d1563a9150e'  # <-- Replace with your real API key
REGION = 'us'  # Available: us, uk, eu, au
MARKETS = 'totals,h2h,spreads'  # Over/Under, Moneyline, Spread
SPORT = 'baseball_mlb'
BOOKMAKERS = 'draftkings,fanduel,pointsbetus'  # Limit for clean output

# Today's date
target_date = datetime.now().strftime('%Y-%m-%d')

# Create output directory
output_dir = "mlb_daily_odds"
os.makedirs(output_dir, exist_ok=True)
filename = f"mlb_betting_odds_{target_date}.csv"
output_path = os.path.join(output_dir, filename)

# === API Request ===
url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
params = {
    'apiKey': API_KEY,
    'regions': REGION,
    'markets': MARKETS,
    'bookmakers': BOOKMAKERS,
    'oddsFormat': 'decimal',
    'dateFormat': 'iso',
}

print(f"📡 Requesting MLB betting odds for {target_date}...")
response = requests.get(url, params=params)

if response.status_code != 200:
    print(f"❌ Failed to fetch data: {response.status_code} - {response.text}")
    exit(1)

odds_json = response.json()

# === Exit gracefully if no data ===
if not odds_json:
    print(f"⚠️ No betting odds returned for {target_date}. Likely no games scheduled. Exiting script.")
    exit(0)

# === Parse Odds Data ===
odds_data = []
for game in odds_json:
    home_team = game.get("home_team", "")
    away_team = game.get("away_team", "")
    game_time = game.get("commence_time", "")[:19].replace("T", " ")

    for bookmaker in game.get("bookmakers", []):
        book_name = bookmaker.get("title", "")
        for market in bookmaker.get("markets", []):
            market_type = market.get("key", "")
            for outcome in market.get("outcomes", []):
                odds_data.append({
                    "Date": target_date,
                    "Game Time": game_time,
                    "Bookmaker": book_name,
                    "Home Team": home_team,
                    "Away Team": away_team,
                    "Market": market_type,
                    "Team": outcome.get("name", ""),
                    "Odds": outcome.get("price", ""),
                    "Point": outcome.get("point", "")
                })

# === Save to CSV ===
df = pd.DataFrame(odds_data)
df.to_csv(output_path, index=False)

print(f"✅ Betting odds saved to {output_path} ({len(odds_data)} rows)")
