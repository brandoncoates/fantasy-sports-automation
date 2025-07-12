import os
import pandas as pd
import requests
from datetime import datetime

# === CONFIG ===
output_dir = "mlb_fanduel_salaries"
os.makedirs(output_dir, exist_ok=True)

today = datetime.now().strftime("%Y-%m-%d")
filename = f"mlb_fanduel_salaries_{today}.csv"
output_path = os.path.join(output_dir, filename)

# === STEP 1: Download CSV from FanDuel (if available)
# ⚠️ FanDuel does not provide a public CSV endpoint like DraftKings.
# This is mock data for now — replace with live scrape/API if available.

data = [
    {"Name": "Aaron Judge", "Team": "NYY", "Position": "OF", "Salary": 4100},
    {"Name": "Shohei Ohtani", "Team": "LAD", "Position": "UTIL", "Salary": 4500},
    {"Name": "Ronald Acuña Jr.", "Team": "ATL", "Position": "OF", "Salary": 4300},
    {"Name": "Gerrit Cole", "Team": "NYY", "Position": "P", "Salary": 10300},
]

df = pd.DataFrame(data)
df.insert(0, "Date", today)

# === STEP 2: Save to CSV
df.to_csv(output_path, index=False)
print(f"✅ Saved {len(df)} FanDuel salaries to {output_path}")
