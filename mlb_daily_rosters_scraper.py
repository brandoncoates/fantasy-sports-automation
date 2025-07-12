import os
import requests
import pandas as pd
from datetime import datetime

# Step 1: Get all teams
teams_url = 'https://statsapi.mlb.com/api/v1/teams?sportId=1'
teams_response = requests.get(teams_url)
teams_data = teams_response.json()

teams = teams_data.get('teams', [])

all_players = []

# Step 2: Loop through teams and get rosters
for team in teams:
    team_id = team['id']
    team_name = team['name']
    
    roster_url = f'https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=all'
    roster_response = requests.get(roster_url)
    if roster_response.status_code != 200:
        print(f"❌ Skipped team {team_name} due to error: {roster_response.status_code}")
        continue
    
    roster_data = roster_response.json().get('roster', [])

    for player in roster_data:
        person = player.get('person', {})
        jersey = player.get('jerseyNumber', '')
        position = player.get('position', {}).get('abbreviation', '')
        status = player.get('status', {}).get('code', 'Unknown')

        all_players.append({
            'Team ID': team_id,
            'Team Name': team_name,
            'Player ID': person.get('id'),
            'Player Name': person.get('fullName'),
            'Jersey Number': jersey,
            'Position': position,
            'Bat Side': person.get('batSide', {}).get('description', ''),
            'Throw Side': person.get('pitchHand', {}).get('description', ''),
            'Status': status
        })

# Step 3: Save to CSV in "MLB Daily Rosters" folder
output_dir = "MLB Daily Rosters"
os.makedirs(output_dir, exist_ok=True)

date_str = datetime.now().strftime('%Y-%m-%d')
filename = f"mlb_rosters_{date_str}.csv"
output_path = os.path.join(output_dir, filename)

df = pd.DataFrame(all_players)
df.to_csv(output_path, index=False)

print(f"✅ Done! Saved {len(all_players)} players to {output_path}")
