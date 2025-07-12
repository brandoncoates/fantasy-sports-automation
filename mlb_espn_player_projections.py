import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# === CONFIG ===
output_dir = "mlb_espn_player_projections"
os.makedirs(output_dir, exist_ok=True)
today = datetime.now().strftime("%Y-%m-%d")
filename = f"mlb_espn_player_projections_{today}.csv"
output_path = os.path.join(output_dir, filename)

# === ESPN Fantasy Projections URL (Batters) ===
url = "https://www.espn.com/fantasy/baseball/story/_/id/39615139/fantasy-baseball-daily-hitter-projections-monday"

# You may need to update this URL daily or scrape from a dynamic feed in the future.
response = requests.get(url)
if response.status_code != 200:
    raise Exception(f"❌ Failed to fetch ESPN projections: {response.status_code}")

soup = BeautifulSoup(response.text, "html.parser")
tables = soup.find_all("table")

if not tables:
    raise Exception("❌ No tables found on the page. ESPN might have changed the layout.")

# === Step 1: Extract Player Projections Table ===
df_list = pd.read_html(str(tables))
proj_df = df_list[0]

# === Step 2: Add Date and Save CSV ===
proj_df.insert(0, "Date", today)
proj_df.to_csv(output_path, index=False)
print(f"✅ Saved {len(proj_df)} ESPN projections to {output_path}")
