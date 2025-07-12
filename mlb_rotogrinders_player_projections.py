import os
import pandas as pd
import requests
from datetime import datetime

# === CONFIG ===
site = "draftkings"  # Change to "fanduel" for FanDuel projections
output_dir = f"mlb_rotogrinders_{site}"
os.makedirs(output_dir, exist_ok=True)
today = datetime.now().strftime("%Y-%m-%d")
filename = f"mlb_rotogrinders_{'dk' if site == 'draftkings' else 'fd'}_{today}.csv"
output_path = os.path.join(output_dir, filename)

# === Step 1: Load projections from Rotogrinders CSV URL
csv_url = f"https://rotogrinders.com/projected-stats/mlb-{site}.csv?site={site}"
response = requests.get(csv_url)

if response.status_code != 200:
    print(f"❌ Failed to fetch projections for {site}. Status code: {response.status_code}")
    exit()

# === Step 2: Read into DataFrame and save
df = pd.read_csv(pd.compat.StringIO(response.text))
df.to_csv(output_path, index=False)

print(f"✅ Saved {len(df)} {site.title()} player projections to {output_path}")
