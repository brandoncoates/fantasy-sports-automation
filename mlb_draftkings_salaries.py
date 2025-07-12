import os
import pandas as pd
import requests
from datetime import datetime

# === CONFIG ===
output_dir = "mlb_draftkings_salaries"
os.makedirs(output_dir, exist_ok=True)
date_str = datetime.now().strftime('%Y-%m-%d')
filename = f"mlb_draftkings_salaries_{date_str}.csv"
output_path = os.path.join(output_dir, filename)

# === Step 1: Download CSV from DraftKings URL
csv_url = "https://dknetwork.draftkings.com/dfs/download/salary-pdfs/mlb-draftkings-salaries.csv"
response = requests.get(csv_url)

if response.status_code != 200:
    raise Exception(f"❌ Failed to fetch salaries CSV. Status code: {response.status_code}")

# === Step 2: Save and parse CSV
with open(output_path, 'wb') as f:
    f.write(response.content)

# Optional sanity check
df = pd.read_csv(output_path)
print(f"✅ Saved {len(df)} salary rows to {output_path}")
