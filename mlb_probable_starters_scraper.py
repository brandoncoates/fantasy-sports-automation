import os
import requests
import pandas as pd
from datetime import datetime

# === CONFIG ===
output_dir = "mlb_probable_starters"
os.makedirs(output_dir, exist_ok=True)

today = datetime.now().strftime("%Y-%m-%d")
filename = f"mlb_probable_starters_{today}.csv"
output_path = os.path.join(output_dir, filename)

# === Step 1: Get today's schedule from MLB API
schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&hydrate=probablePitcher(note,stats,person)"
response = requests.get(schedule_url)
data = response.json()

# === Step 2: Extract probable starters
games = data.get('dates', [])[0].get('games', []) if data.get('dates') else []
starters_data = []

for game in games:
    game_id = game.get("gamePk")
    game_date = game.get("gameDate")
    teams = game.get("teams", {})

    for team_type in ["home", "away"]:
        team_info = teams.get(team_type, {})
        team_name = team_info.get("team", {}).get("name", "")
        pitcher_info = team_info.get("probablePitcher", {})

        if pitcher_info:
            starters_data.append({
                "Game ID": game_id,
                "Game Date": game_date,
                "Team": team_name,
                "Pitcher Name": pitcher_info.get("fullName"),
                "Pitcher ID": pitcher_info.get("id"),
                "Handedness": pitcher_info.get("pitchHand", {}).get("code"),
            })

# === Step 3: Save to CSV
df = pd.DataFrame(starters_data)
df.to_csv(output_path, index=False)
print(f"✅ Saved {len(df)} probable starters to {output_path}")
