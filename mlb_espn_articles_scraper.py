import os
import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import boto3

# === AWS / S3 CONFIG ===
BUCKET     = os.environ["S3_BUCKET_NAME"]    # e.g. "fantasy-sports-csvs"
REGION     = os.environ.get("AWS_REGION", "us-east-1")
S3_FOLDER  = "news"

# === OUTPUT CONFIG ===
today        = datetime.now().strftime("%Y-%m-%d")
json_filename = f"mlb_espn_articles_{today}.json"
local_dir     = "mlb_espn_articles"
os.makedirs(local_dir, exist_ok=True)
local_path    = os.path.join(local_dir, json_filename)
s3_key        = f"{S3_FOLDER}/{json_filename}"

# === SCRAPE ESPN MLB PAGE ===
url     = "https://www.espn.com/mlb/"
headers = {"User-Agent": "Mozilla/5.0"}
resp    = requests.get(url, headers=headers)
resp.raise_for_status()

soup    = BeautifulSoup(resp.text, "html.parser")
articles = []
for link in soup.find_all("a", href=True):
    headline = link.get_text(strip=True)
    href     = link["href"]
    if (
        "/mlb/story" in href
        and headline
        and not href.startswith(("javascript:", "#"))
    ):
        full_url = href if href.startswith("http") else f"https://www.espn.com{href}"
        articles.append({
            "date":     today,
            "headline": headline,
            "url":      full_url
        })

# Deduplicate by URL
unique   = {art["url"]: art for art in articles}
cleaned  = list(unique.values())

# === SAVE TO JSON LOCALLY ===
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(cleaned, f, ensure_ascii=False, indent=2)
print(f"💾 Saved {len(cleaned)} ESPN articles to {local_path}")

# === UPLOAD TO S3 ===
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(local_path, BUCKET, s3_key)
    print(f"☁️ Uploaded to s3://{BUCKET}/{s3_key}")
except Exception as e:
    print(f"❌ Failed to upload to S3: {e}")
    exit(1)
