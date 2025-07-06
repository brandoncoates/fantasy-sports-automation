import requests
import pandas as pd
from datetime import datetime

# Set up target date
target_date = datetime.now().strftime('%Y-%m-%d')

# Save location
save_path = r'C:\Users\brand\OneDrive\Documents\Python Projects\Fantasy Baseball\MLB Daily Rosters'
filename = f"{save_path}\\mlb_rosters_{target_date}.csv"

# Hardcoded Team IDs and Names (bulletproof)
teams = {
    108: 'Angels', 109: 'Diamondbacks', 110: 'Braves', 111: 'Orioles',
    112: 'Red Sox', 113: 'Cubs', 114: 'White Sox', 115: 'Reds',
    116: 'Guardians', 117: 'Rockies', 118: 'Tigers', 119: 'Astros',
    120: 'Royals', 121: 'Dodgers', 133: 'Marlins', 134: 'Brewers',
    135: 'Twins', 136: 'Yankees', 137: 'Mets', 138: 'Athletics',
    139: 'Phillies', 140: 'Pirates', 141: 'Padres', 142: 'Giants',
    143: 'Mariners', 144: 'Cardinals', 145: 'Rays', 146: 'Rangers',
    147: 'Blue Jays', 158: 'Nationals'
}

all_players = []

# Fetch each team's roster
for team_id, team_name in teams.items():
    print(f"🔄 Fetching roster for {team_name} (ID {team_id})")

    try:
        roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=fullRoster"
        roster_response = requests.get(roster_url)
        roster_data = roster_response.json().get("roster", [])

        for player in roster_data:
            person = player.get("person", {})
            player_id = person.get("id", "")
            player_name = person.get("fullName", "")
            jersey_number = player.get("jerseyNumber", "")
            position = player.get("position", {}).get("abbreviation", "")
            position_type = player.get("position", {}).get("type", "")
            status = player.get("status", {}).get("description", "")

            # Get player profile for bat/throw (may or may not exist)
            profile_url = f"https://statsapi.mlb.com/api/v1/people/{player_id}"
            profile_resp = requests.get(profile_url)
            profile_data = profile_resp.json().get("people", [{}])[0]

            bat_side = profile_data.get("batSide", {}).get("description", "Unknown")
            throw_side = profile_data.get("pitchHand", {}).get("description", "Unknown")

            player_row = {
                "Team ID": str(team_id),
                "Team Name": str(team_name),  # <- FORCED NAME HERE
                "Player ID": str(player_id),
                "Player Name": str(player_name),
                "Jersey Number": str(jersey_number),
                "Position": str(position),
                "Position Type": str(position_type),
                "Bat Side": str(bat_side) or "Unknown",
                "Throw Side": str(throw_side) or "Unknown",
                "Status": str(status)
            }

            all_players.append(player_row)

    except Exception as e:
        print(f"⚠️ Error processing {team_name} ({team_id}): {e}")

# 🚨 DEBUG: Check first 5 rows before saving
print("\n🚨 First 5 rows before saving:")
for row in all_players[:5]:
    print(row)

# Create DataFrame with locked columns
columns = [
    "Team ID", "Team Name", "Player ID", "Player Name", "Jersey Number",
    "Position", "Position Type", "Bat Side", "Throw Side", "Status"
]

df = pd.DataFrame(all_players, columns=columns)
df.to_csv(filename, index=False)

print(f"\n✅ Done! Saved {len(df)} players to {filename}")
