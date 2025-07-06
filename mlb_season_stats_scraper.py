import requests
import pandas as pd
from datetime import datetime

# Set up target date for filename
target_date = datetime.now().strftime('%Y-%m-%d')
save_path = r'C:\Users\brand\OneDrive\Documents\Python Projects\Fantasy Baseball\MLB Season Stats'
filename = f"{save_path}\\mlb_season_stats_{target_date}.csv"

# Hardcoded Team IDs and Names
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

for team_id, team_name in teams.items():
    print(f"🔄 Fetching players from {team_name}...")

    try:
        roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=fullRoster"
        roster_resp = requests.get(roster_url)
        roster_data = roster_resp.json().get('roster', [])

        for player in roster_data:
            person = player.get("person", {})
            player_id = person.get("id", "")
            player_name = person.get("fullName", "")
            jersey_number = player.get("jerseyNumber", "")
            position = player.get("position", {}).get("abbreviation", "")
            position_type = player.get("position", {}).get("type", "")
            status = player.get("status", {}).get("description", "")

            profile_url = f"https://statsapi.mlb.com/api/v1/people/{player_id}?hydrate=stats(group=[hitting,pitching],type=season)"
            profile_resp = requests.get(profile_url)
            profile_data = profile_resp.json().get('people', [{}])[0]

            bat_side = profile_data.get("batSide", {}).get("description", "Unknown")
            throw_side = profile_data.get("pitchHand", {}).get("description", "Unknown")

            # Extract season hitting stats
            hitting_stats = {}
            pitching_stats = {}

            stats_list = profile_data.get('stats', [])

            for stats in stats_list:
                if stats.get('group', {}).get('displayName') == 'hitting':
                    hitting_stats = stats.get('stats', {})
                elif stats.get('group', {}).get('displayName') == 'pitching':
                    pitching_stats = stats.get('stats', {})

            player_row = {
                "Team ID": str(team_id),
                "Team Name": str(team_name),
                "Player ID": str(player_id),
                "Player Name": str(player_name),
                "Jersey Number": str(jersey_number),
                "Position": str(position),
                "Position Type": str(position_type),
                "Bat Side": str(bat_side) or "Unknown",
                "Throw Side": str(throw_side) or "Unknown",
                "Status": str(status),
                # Hitting
                "AVG": hitting_stats.get('avg', ''),
                "OBP": hitting_stats.get('obp', ''),
                "SLG": hitting_stats.get('slg', ''),
                "OPS": hitting_stats.get('ops', ''),
                "HR": hitting_stats.get('homeRuns', ''),
                "RBI": hitting_stats.get('rbi', ''),
                "SB": hitting_stats.get('stolenBases', ''),
                # Pitching
                "ERA": pitching_stats.get('era', ''),
                "W": pitching_stats.get('wins', ''),
                "L": pitching_stats.get('losses', ''),
                "SV": pitching_stats.get('saves', ''),
                "IP": pitching_stats.get('inningsPitched', ''),
                "K": pitching_stats.get('strikeOuts', '')
            }

            all_players.append(player_row)

    except Exception as e:
        print(f"⚠️ Error processing team {team_name} (ID {team_id}): {e}")

# Create DataFrame and Save
df = pd.DataFrame(all_players)
df.to_csv(filename, index=False)

print(f"\n✅ Done! Saved {len(df)} player rows to {filename}")
