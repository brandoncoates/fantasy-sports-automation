import os
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# === CONFIG ===
output_dir = "mlb_espn_articles"
os.makedirs(output_dir, exist_ok=True)
today = datetime.now().strftime("%Y-%m-%d")
filename = f"mlb_espn_articles_{today}.csv"
output_path = os.path.join(output_dir, filename)

# === Step 1: Define URL and fetch page ===
url = "https://www.espn.com/mlb/"
response = requests.get(url)

if response.status_code != 200:
    raise Exception(f"❌ Failed to fetch ESPN MLB page: {response.status_code}")

soup = BeautifulSoup(response.text, "html.parser")
articles = []

# === Step 2: Find article blocks ===
for link in soup.find_all("a", href=True):
    headline = link.get_text(strip=True)
    href = link["href"]

    if (
        "/mlb/story" in href
        and headline
        and not href.startswith("javascript:")
        and not href.startswith("#")
    ):
        full_url = href if href.startswith("http") else f"https://www.espn.com{href}"
        articles.append({
            "Date": today,
            "Headline": headline,
            "URL": full_url
        })

# === Step 3: Save to CSV ===
df = pd.DataFrame(articles).drop_duplicates(subset=["URL"])
df.to_csv(output_path, index=False)
print(f"✅ Saved {len(df)} articles to {output_path}")
