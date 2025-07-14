import requests
import pandas as pd
from datetime import date
import os

# Load API key from GitHub Secrets
ODDS_API_KEY = os.getenv("ODDS_API_KEY")

# API endpoint for MLB player props
url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"

params = {
    "apiKey": ODDS_API_KEY,
    "regions": "us",  # Only US books
    "markets": "player_hits,player_home_runs,player_rbis,player_strikeouts",
    "oddsFormat": "decimal",
    "dateFormat": "iso",
    "bookmakers": "draftkings,betmgm,fanduel"
}

response = requests.get(url, params=params)

if response.status_code != 200:
    print(f"Request failed with status code {response.status_code}: {response.text}")
    exit()

data = response.json()
props_list = []

for game in data:
    game_time = game.get("commence_time", "")
    teams = game.get("teams", [])
    bookmakers = game.get("bookmakers", [])
    
    for book in bookmakers:
        book_name = book.get("title", "")
        markets = book.get("markets", [])
        
        for market in markets:
            market_key = market.get("key", "")
            for outcome in market.get("outcomes", []):
                props_list.append({
                    "Game Time": game_time,
                    "Team 1": teams[0] if len(teams) > 0 else "",
                    "Team 2": teams[1] if len(teams) > 1 else "",
                    "Bookmaker": book_name,
                    "Prop Type": market_key,
                    "Player": outcome.get("name", ""),
                    "Line": outcome.get("point", ""),
                    "Odds": outcome.get("price", "")
                })

if not props_list:
    print("No player props found.")
else:
    today = date.today().strftime("%Y-%m-%d")
    df = pd.DataFrame(props_list)
    filename = f"oddsapi_player_props_{today}.csv"
    df.to_csv(filename, index=False)
    print(f"✅ Saved {len(df)} props to {filename}")

    # Upload to S3
    import boto3

    s3 = boto3.client("s3")
    BUCKET_NAME = "goatland-csv-storage"
    s3.upload_file(filename, BUCKET_NAME, f"baseball/props/oddsapi_player_props_{today}.csv")
    print("✅ Uploaded to S3")
