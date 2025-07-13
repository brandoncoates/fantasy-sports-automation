import os
import pandas as pd
import requests
from datetime import datetime
from io import StringIO

# === CONFIG ===
output_dir = "mlb_rotogrinders_projections"
os.makedirs(output_dir, exist_ok=True)

today = datetime.now().strftime("%Y-%m-%d")

sites = {
    "draftkings": "dk",
    "fanduel": "fd"
}

# === Loop through both DK and FD
for site, short in sites.items():
    filename = f"mlb_rotogrinders_{short}_{today}.csv"
    output_path = os.path.join(output_dir, filename)

    csv_url = f"https://rotogrinders.com/projected-stats/mlb-{site}.csv?site={site}"
    response = requests.get(csv_url)

    if response.status_code != 200:
        print(f"❌ Failed to fetch projections for {site}. Status code: {response.status_code}")
        continue

    try:
        df = pd.read_csv(StringIO(response.text))
        df.to_csv(output_path, index=False)
        print(f"✅ Saved {len(df)} {site.title()} player projections to {output_path}")
    except Exception as e:
        print(f"❌ Error parsing {site} projections: {e}")
