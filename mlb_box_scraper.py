import os
import requests
import json
from datetime import datetime, timedelta

# import the normalization function
from shared.normalize_name import normalize_name

# === CONFIG: Set True to pull yesterday's games ===
use_yesterday = True

# Get date in YYYY-MM-DD format
if use_yesterday:
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
else:
    target_date = datetime.now().strftime('%Y-%m-%d')

# Step 1: Get Schedule for the target date
schedule_url = f'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={target_date}'
response   = requests.get(schedule_url)
schedule   = response.json().get('dates', [])
games      = schedule[0].get('games', []) if schedule else []
game_ids   = [game['gamePk'] for game in games]

print(f"Found {len(game_ids)} games for {target_date}.")

all_boxscores = []

# Step 2: Loop through each game's boxscore
for game_id in game_ids:
    try:
        boxscore_url = f'https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore'
        box_data     = requests.get(boxscore_url).json()
        teams        = box_data['teams']

        for team_key in ['home', 'away']:
            team_info = teams[team_key]['team']
            team_name = team_info['name']

            players = teams[team_key]['players']
            for player in players.values():
                person   = player.get('person', {})
                batting  = player.get('stats', {}).get('batting', {})
                pitching = player.get('stats', {}).get('pitching', {})

                # normalize the player name
                raw_name = person.get('fullName', "")
                player_name = normalize_name(raw_name)

                all_boxscores.append({
                    'Game Date':       target_date,
                    'Game ID':         game_id,
                    'Team':            team_name,
                    'Player Name':     player_name,
                    'Position':        ', '.join(pos['abbreviation'] for pos in player.get('allPositions', [])) if player.get('allPositions') else '',
                    
                    # Batting Stats:
                    'Batting AVG':         batting.get('avg'),
                    'At Bats':              batting.get('atBats'),
                    'Hits':                 batting.get('hits'),
                    'Doubles':              batting.get('doubles'),
                    'Triples':              batting.get('triples'),
                    'Home Runs':            batting.get('homeRuns'),
                    'RBIs':                 batting.get('rbi'),
                    'Runs':                 batting.get('runs'),
                    'Walks':                batting.get('baseOnBalls'),
                    'Strikeouts (Batting)': batting.get('strikeOuts'),
                    'Stolen Bases':         batting.get('stolenBases'),
                    'Left On Base':         batting.get('leftOnBase'),
                    'OBP':                  batting.get('obp'),
                    'SLG':                  batting.get('slg'),
                    'OPS':                  batting.get('ops'),
                    
                    # Pitching Stats:
                    'Pitcher ERA':             pitching.get('era'),
                    'Innings Pitched':         pitching.get('inningsPitched'),
                    'Hits Allowed':            pitching.get('hits'),
                    'Runs Allowed':            pitching.get('runs'),
                    'Earned Runs':             pitching.get('earnedRuns'),
                    'Walks Issued':            pitching.get('baseOnBalls'),
                    'Strikeouts (Pitching)':   pitching.get('strikeOuts'),
                    'Home Runs Allowed':       pitching.get('homeRuns'),
                    'Pitches Thrown':          pitching.get('numberOfPitches'),
                    'Strikes':                 pitching.get('strikes'),
                    'Wins':                    pitching.get('wins'),
                    'Losses':                  pitching.get('losses'),
                    'Saves':                   pitching.get('saves'),
                    'Holds':                   pitching.get('holds'),
                    'Blown Saves':             pitching.get('blownSaves'),
                })
    except Exception as e:
        print(f"❌ Skipped game {game_id} due to error: {e}")

# ✅ Step 3: Save to JSON in mlb_box_scores/
output_dir = "mlb_box_scores"
os.makedirs(output_dir, exist_ok=True)

filename    = f"mlb_boxscores_{target_date}.json"
output_path = os.path.join(output_dir, filename)

with open(output_path, mode="w", encoding="utf-8") as f:
    json.dump(all_boxscores, f, ensure_ascii=False, indent=2)

print(f'✅ Saved {len(all_boxscores)} player entries to {output_path}')
