import requests
import pandas as pd
from datetime import datetime
import pytz

# === Config ===
# Always use Eastern Time to get the correct "today's" date for MLB
eastern = pytz.timezone('US/Eastern')
now_eastern = datetime.now(eastern)
target_date = now_eastern.strftime('%Y%m%d')  # ESPN API needs YYYYMMDD
csv_date = now_eastern.strftime('%Y-%m-%d')
filename = f"mlb_probable_starters_{csv_date}.csv"

# Step 1: Pull ESPN Scoreboard for specific date
espn_url = f'https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={target_date}'
response = requests.get(espn_url)
espn_data = response.json()

all_games = []

for event in espn_data.get('events', []):
    competition = event.get('competitions', [])[0]
    competitors = competition.get('competitors', [])

    home_team = away_team = home_pitcher = away_pitcher = ''

    for team in competitors:
        team_name = team.get('team', {}).get('displayName', '')
        is_home = team.get('homeAway') == 'home'

        # Get probable pitcher or fallback to 'TBD'
        probables = team.get('probables', [])
        pitcher_name = probables[0].get('athlete', {}).get('displayName') if probables else 'TBD'

        if is_home:
            home_team = team_name
            home_pitcher = pitcher_name
        else:
            away_team = team_name
            away_pitcher = pitcher_name

    # Game Time conversion to Eastern Time
    game_time_utc = competition.get('date', '')
    if game_time_utc:
        try:
            utc_dt = datetime.strptime(game_time_utc[:16], "%Y-%m-%dT%H:%M")
            utc_dt = utc_dt.replace(tzinfo=pytz.utc)
            est_dt = utc_dt.astimezone(eastern)
            game_time_local = est_dt.strftime('%I:%M %p ET')
            game_date_local = est_dt.strftime('%Y-%m-%d')
        except Exception:
            game_time_local = ''
            game_date_local = csv_date
    else:
        game_time_local = ''
        game_date_local = csv_date

    matchup = f"{away_team} @ {home_team}"

    all_games.append({
        'Date': game_date_local,
        'Matchup': matchup,
        'Game Time (ET)': game_time_local,
        'Away Probable Pitcher': away_pitcher,
        'Home Probable Pitcher': home_pitcher
    })

# Step 2: Save to CSV in current directory
df = pd.DataFrame(all_games)
df.to_csv(filename, index=False)

print(f"✅ Done! Saved {len(df)} games for {csv_date} to {filename}")

