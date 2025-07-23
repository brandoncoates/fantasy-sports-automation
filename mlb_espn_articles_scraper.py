import os
import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup

# === CONFIG ===
output_dir = "mlb_espn_articles"
os.makedirs(output_dir, exist_ok=True)

today = datetime.now().strftime("%Y-%m-%d")
json_filename = f"mlb_espn_articles_{today}.json"
output_path = os.path.join(output_dir, json_filename)

# === Step 1: Fetch ESPN MLB page ===
url = "https://www.espn.com/mlb/"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)
if response.status_code != 200:
    raise Exception(f"❌ Failed to fetch ESPN MLB page: {response.status_code}")

soup = BeautifulSoup(response.text, "html.parser")

# === Step 2: Extract articles ===
articles = []
for link in soup.find_all("a", href=True):
    headline = link.get_text(strip=True)
    href = link["href"]

    if (
        "/mlb/story" in href
        and headline
        and not href.startswith(("javascript:", "#"))
    ):
        full_url = href if href.startswith("http") else f"https://www.espn.com{href}"
        articles.append({
            "Date":     today,
            "Headline": headline,
            "URL":      full_url
        })

# Dedupe by URL
unique = {art["URL"]: art for art in articles}
cleaned = list(unique.values())

# === Step 3: Save to JSON ===
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(cleaned, f, ensure_ascii=False, indent=2)

print(f"✅ Saved {len(cleaned)} articles to {output_path}")
