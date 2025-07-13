import os
import csv
import requests
import boto3
from bs4 import BeautifulSoup
from datetime import datetime

# CONFIG
REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/salaries"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_rotowire_salaries_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

url = "https://www.rotowire.com/daily/mlb/player-roster-percent.php?site=DraftKings"

print("📡 Fetching Rotowire roster % page...")
resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
if resp.status_code != 200:
    print(f"❌ HTTP {resp.status_code}")
    exit(1)

soup = BeautifulSoup(resp.text, "html.parser")
table = soup.find("table")
if not table:
    print("❌ No table found on page.")
    exit(1)

print("🔍 Parsing table rows...")
rows = []
headers = [th.text.strip() for th in table.find_all("thead")[0].find_all("th")]

for tr in table.find("tbody").find_all("tr"):
    cols = [td.text.strip() for td in tr.find_all("td")]
    if len(cols) != len(headers):
        continue
    rows.append(dict(zip(headers, cols)))

if not rows:
    print("⚠️ No data rows found.")
    exit(1)

print(f"✅ Got {len(rows)} rows. Writing CSV...")

with open(FILENAME, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)

print(f"☁️ Uploading to S3: {BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(FILENAME, BUCKET, S3_KEY)
    print("✅ Upload OK")
except Exception as e:
    print("❌ Upload failed:", e)
    exit(1)

os.remove(FILENAME)
print("🧹 Cleaned up temp file.")
