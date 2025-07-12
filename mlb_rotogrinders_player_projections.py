import pandas as pd
from datetime import datetime
import os

# Today's date
today = datetime.today().strftime('%Y-%m-%d')

# Output directory
output_dir = r"C:\Users\brand\OneDrive\Documents\Python Projects\Fantasy Baseball\Rotogrinders Player Projections"
os.makedirs(output_dir, exist_ok=True)

# URLs
urls = {
    "DraftKings": "https://www.rotogrinders.com/projected-stats/mlb-hitter.csv?site=draftkings",
    "FanDuel": "https://www.rotogrinders.com/projected-stats/mlb-hitter.csv?site=fanduel"
}

# Loop through each site and save CSV
for site, url in urls.items():
    try:
        df = pd.read_csv(url)
        file_path = os.path.join(output_dir, f"mlb_rotogrinders_{site.lower()}_projections_{today}.csv")
        df.to_csv(file_path, index=False)
        print(f"✅ {site} projections saved to: {file_path}")
    except Exception as e:
        print(f"❌ Failed to fetch {site} projections: {e}")
