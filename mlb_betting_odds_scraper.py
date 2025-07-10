import requests
import pandas as pd
from datetime import datetime
import pytz

# === CONFIG ===
API_KEY = '32c95ea767253beab2da2d1563a9150e'  # Replace with your actual Odds API key
SPORT = 'baseball_mlb'
REGION = 'us'
MARKETS = 'h2h,spreads,totals'

# Get current date in Eastern Time
eastern = pytz.timezone('US/Eastern')
current_date = datetime.now(eastern).strftime('%Y-%m-%d')

# File name
filename = f"mlb_betting_odds_{current_date}.csv"

# Odds API endpoint
url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds/?apiKey={API_KEY}&regions={REGION}&markets={MARKETS}&oddsFormat=american"

response = requests.get(url)
if response.status_code != 200:
    print(f"❌ Failed to fetch data: {response.status_code} - {response.text}")
    exit()

games = response.json()

all_odds = []

for game in games:
    home_team = game.get('home_team', '')
    away_team = game.get('away_team', '')
    commence_time_utc = game.get('commence_time', '')

    # Convert time to Eastern
    try:
        utc_time = datetime.strptime(commence_time_utc, "%Y-%m-%dT%H:%M:%SZ")
        eastern_time = utc_time.replace(tzinfo=pytz.utc).astimezone(eastern)
        game_time = eastern_time.strftime('%Y-%m-%d %I:%M %p %Z')
    except:
        game_time = ''

    for bookmaker in game.get('bookmakers', []):
        site = bookmaker.get('title', '')

        h2h = bookmaker.get('markets', [])[0].get('outcomes', []) if bookmaker.get('markets') else []
        spreads = next((m['outcomes'] for m in bookmaker.get('markets', []) if m['key'] == 'spreads'), [])
        totals = next((m['outcomes'] for m in bookmaker.get('markets', []) if m['key'] == 'totals'), [])

        # Prepare odds dict
        odds_entry = {
            'Game': f"{away_team} @ {home_team}",
            'Game Time (ET)': game_time,
            'Sportsbook': site
        }

        # Moneyline
        for outcome in h2h:
            if outcome['name'] == home_team:
                odds_entry['Home Moneyline'] = outcome['price']
            elif outcome['name'] == away_team:
                odds_entry['Away Moneyline'] = outcome['price']

        # Spread
        for outcome in spreads:
            if outcome['name'] == home_team:
                odds_entry['Home Spread'] = outcome.get('point', '')
                odds_entry['Home Spread Odds'] = outcome.get('price', '')
            elif outcome['name'] == away_team:
                odds_entry['Away Spread'] = outcome.get('point', '')
                odds_entry['Away Spread Odds'] = outcome.get('price', '')

        # Total
        for outcome in totals:
            if 'Over' in outcome['name']:
                odds_entry['Over'] = outcome.get('point', '')
                odds_entry['Over Odds'] = outcome.get('price', '')
            elif 'Under' in outcome['name']:
                odds_entry['Under'] = outcome.get('point', '')
                odds_entry['Under Odds'] = outcome.get('price', '')

        all_odds.append(odds_entry)

# Save to CSV
df = pd.DataFrame(all_odds)
df.to_csv(filename, index=False)
print(f"✅ Betting odds saved to {filename} ({len(df)} games)")
