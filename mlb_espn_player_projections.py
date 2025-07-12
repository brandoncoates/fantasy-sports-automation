import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time

# Output setup
today = datetime.today().strftime('%Y-%m-%d')
output_dir = r"C:\Users\brand\OneDrive\Documents\Python Projects\Fantasy Baseball\ESPN Player Projections"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, f"mlb_espn_player_projections_{today}.csv")

# ESPN Fantasy MLB projections (HTML tables paginated by offset)
base_url = "https://fantasy.espn.com/baseball/players/projections"
params = {
    "leagueId": 0,
    "seasonId": 2025,
    "slotCategoryGroup": "MLB",
    "startIndex": 0
}

headers = {
    "User-Agent": "Mozilla/5.0"
}

all_players = []

while True:
    print(f"Fetching page starting at index {params['startIndex']}...")

    response = requests.get(base_url, params=params, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch data at index {params['startIndex']}")
        break

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table')

    if not table:
        print("No more data or table not found.")
        break

    df = pd.read_html(str(table))[0]

    if df.empty:
        break

    all_players.append(df)
    params["startIndex"] += 40  # ESPN paginates by 40 players per page
    time.sleep(1.5)  # Be kind to ESPN's servers

# Combine all player projections
if all_players:
    projections_df = pd.concat(all_players, ignore_index=True)
    projections_df.to_csv(output_file, index=False)
    print(f"✅ Saved: {output_file}")
else:
    print("❌ No data found.")
