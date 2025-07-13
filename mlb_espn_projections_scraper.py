import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# === CONFIG ===
base_url = "https://www.espn.com"
fantasy_home = f"{base_url}/fantasy/baseball/"
target_date = datetime.now().strftime('%Y-%m-%d')
output_dir = "mlb_player_projections"
filename = f"mlb_espn_projections_{target_date}.csv"
output_path = os.path.join(output_dir, filename)

# === Ensure output directory exists ===
os.makedirs(output_dir, exist_ok=True)

# === Step 1: Load Fantasy Baseball homepage ===
headers = {"User-Agent": "Mozilla/5.0"}
home_response = requests.get(fantasy_home, headers=headers)
home_soup = BeautifulSoup(home_response.text, "html.parser")

# === Step 2: Find the most recent "hitter projections" article ===
article_url = None
for link in home_soup.find_all("a", href=True):
    href = link['href']
    text = link.get_text(strip=True).lower()
    if "hitter projections" in href or "hitter projections" in text:
        article_url = href
        if not article_url.startswith("http"):
            article_url = base_url + article_url
        break

if not article_url:
    print("❌ Could not find ESPN hitter projections article.")
    exit()

print(f"🔗 Found article: {article_url}")

# === Step 3: Load article and look for tables ===
article_response = requests.get(article_url, headers=headers)
article_soup = BeautifulSoup(article_response.text, "html.parser")
tables = article_soup.find_all("table")

if not tables:
    print("❌ No tables found in the article.")
    exit()

# === Step 4: Parse the first table as DataFrame ===
try:
    df = pd.read_html(str(tables[0]))[0]
    df.columns = [col.strip() for col in df.columns]  # Clean headers
except Exception as e:
    print(f"❌ Error reading table: {e}")
    exit()

# === Step 5: Save to CSV ===
df.to_csv(output_path, index=False)
print(f"✅ ESPN hitter projections saved to {output_path} ({len(df)} rows)")
