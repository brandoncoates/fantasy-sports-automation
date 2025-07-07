import requests
import pandas as pd
from datetime import datetime, timedelta

# === CONFIG: Set True to pull yesterday's games ===
use_yesterday = True

# Get date in YYYY-MM-DD format
if use_yesterday:
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
else:
    target_date = datetime.now().strftime('%Y-%m-%d')

# Step 1: Get Schedule for the target date
schedule_url = f'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={target_date}'
response = requests.get(schedule_url)
schedule_data = response.json()

games = schedule_data.get('dates', [])[0].get('games', []) if schedule_data.get('dates') else []
game_ids = [game['gamePk'] for game in games]

print(f"Found {len(game_ids)} games for {target_date}.")

all_boxscores = []

# Step 2: Loop through each game's boxscore
for game_id in game_ids:
    try:
        boxscore_url = f'https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore'
        box_response = requests.get(boxscore_url)
        box_data = box_response.json()

        teams = box_data['teams']

        for team_key in ['home', 'away']:
            team_info = teams[team_key]['team']
            team_name = team_info['name']

            players = teams[team_key]['players']
            for player_id, player in players.items():
                person = player.get('person', {})
                stats = player.get('stats', {})

                batting = stats.get('batting', {})
                pitching = stats.get('pitching', {})

                all_boxscores.append({
                    'Game Date': target_date,
                    'Game ID': game_id,
                    'Team': team_name,
                    'Player Name': person.get('fullName'),
                    'Position': ', '.join(pos['abbreviation'] for pos in player.get('allPositions', [])) if player.get('allPositions') else '',
                    
                    # Batting Stats:
                    'Batting AVG': batting.get('avg'),
                    'At Bats': batting.get('atBats'),
                    'Hits': batting.get('hits'),
                    'Doubles': batting.get('doubles'),
                    'Triples': batting.get('triples'),
                    'Home Runs': batting.get('homeRuns'),
                    'RBIs': batting.get('rbi'),
                    'Runs': batting.get('runs'),
                    'Walks': batting.get('baseOnBalls'),
                    'Strikeouts (Batting)': batting.get('strikeOuts'),
                    'Stolen Bases': batting.get('stolenBases'),
                    'Left On Base': batting.get('leftOnBase'),
                    'OBP': batting.get('obp'),
                    'SLG': batting.get('slg'),
                    'OPS': batting.get('ops'),
                    
                    # Pitching Stats:
                    'Pitcher ERA': pitching.get('era'),
                    'Innings Pitched': pitching.get('inningsPitched'),
                    'Hits Allowed': pitching.get('hits'),
                    'Runs Allowed': pitching.get('runs'),
                    'Earned Runs': pitching.get('earnedRuns'),
                    'Walks Issued': pitching.get('baseOnBalls'),
                    'Strikeouts (Pitching)': pitching.get('strikeOuts'),
                    'Home Runs Allowed': pitching.get('homeRuns'),
                    'Pitches Thrown': pitching.get('numberOfPitches'),
                    'Strikes': pitching.get('strikes'),
                    'Wins': pitching.get('wins'),
                    'Losses': pitching.get('losses'),
                    'Saves': pitching.get('saves'),
                    'Holds': pitching.get('holds'),
                    'Blown Saves': pitching.get('blownSaves'),
                })
    except Exception as e:
        print(f"❌ Skipped game {game_id} due to error: {e}")

# Step 3: Save to CSV in current directory (no file path)
df = pd.DataFrame(all_boxscores)
filename = f'mlb_boxscores_{target_date}.csv'
df.to_csv(filename, index=False)

print(f'✅ Saved {len(df)} player rows to {filename}')

