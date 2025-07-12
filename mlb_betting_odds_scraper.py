import os
import requests
import pandas as pd
from datetime import datetime

# === CONFIG: Set True to pull today's games ===
use_today = True

# Get date in YYYY-MM-DD format
target_date = datetime.now().strftime('%Y-%m-%d')

# Step 1: Get Schedule for the target date
schedule_url = f'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={target_date}'
response = requests.get(schedule_url)
schedule_data = response.json()

games = schedule_data.get('dates', [])[0].get('games', []) if schedule_data.get('dates') else []
game_ids = [game['gamePk'] for game in games]

print(f"Found {len(game_ids)} games for {target_date}.")

odds_data = []

# Step 2: Loop through each game's odds
for game_id in game_ids:
    try:
        odds_url = f'https://statsapi.mlb.com/api/v1/game/{game_id}/linescore'
        odds_response = requests.get(odds_url)
        odds_json = odds_response.json()

        game_url = f'https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live'
        game_response = requests.get(game_url)
        game_data = game_response.json()

        game_info = game_data.get("gameData", {})
        teams_info = game_info.get("teams", {})
        home_team = teams_info.get("home", {}).get("name", "")
        away_team = teams_info.get("away", {}).get("name", "")

        # Extract betting info from metadata
        metadata = game_data.get("gameData", {}).get("metadata", {})
        betting_info = game_data.get("liveData", {}).get("boxscore", {}).get("info", [])

        # Fallback: some odds might be embedded in 'info' or 'metadata'
        odds_entry = {
            "Game ID": game_id,
            "Date": target_date,
            "Home Team": home_team,
            "Away Team": away_team,
            "Spread": None,
            "Over/Under": None
        }

        for item in betting_info:
            label = item.get("label", "").lower()
            value = item.get("value", [""])[0]

            if "over/under" in label:
                odds_entry["Over/Under"] = value
            elif "spread" in label:
                odds_entry["Spread"] = value

        odds_data.append(odds_entry)

    except Exception as e:
        print(f"❌ Skipped game {game_id} due to error: {e}")

# Step 3: Save to folder
output_dir = "MLB Daily Odds"
os.makedirs(output_dir, exist_ok=True)

filename = f"mlb_betting_odds_{target_date}.csv"
output_path = os.path.join(output_dir, filename)

df = pd.DataFrame(odds_data)
df.to_csv(output_path, index=False)

print(f"✅ Betting odds saved to {output_path} ({len(odds_data)} games)")
