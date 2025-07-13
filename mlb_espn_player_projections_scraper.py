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

# === Step 1: Get main fantasy baseball page
main_url = "https://www.espn.com/fantasy/baseball/"
main_response = requests.get(main_url)
if main_response.status_code != 200:
    raise Exception(f"❌ Failed to fetch ESPN Fantasy Baseball homepage: {main_response.status_code}")

main_soup = BeautifulSoup(main_response.text, "html.parser")

# === Step 2: Find latest hitter projections article
article_url = None
for link in main_soup.find_all("a", href=True):
    href = link["href"]
    if "daily-hitter-projections" in href:
        article_url = href if href.startswith("http") else f"https://www.espn.com{href}"
        break

if not article_url:
    raise Exception("❌ Could not locate hitter projections article on ESPN fantasy page.")

# === Step 3: Scrape table from projections article
article_response = requests.get(article_url)
if article_response.status_code != 200:
    raise Exception(f"❌ Failed to fetch projections article: {article_response.status_code}")

article_soup = BeautifulSoup(article_response.text, "html.parser")
tables = article_soup.find_all("table")

if not tables:
    raise Exception("❌ No tables found in ESPN projections article.")

df_list = pd.read_html(str(tables))
proj_df = df_list[0]

# === Step 4: Add date and save
proj_df.insert(0, "Date", today)
proj_df.to_csv(output_path, index=False)
print(f"✅ Saved {len(proj_df)} ESPN projections to {output_path}")
