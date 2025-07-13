import os
import csv
import requests
import boto3
from datetime import datetime
from bs4 import BeautifulSoup

# === CONFIG ===
REGION = "us-east-2"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/salaries"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_rotowire_salaries_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === STEP 1: Fetch the Rotowire page ===
print("📡 Fetching Rotowire Salary + Roster % page...")
url = "https://www.rotowire.com/daily/mlb/player-roster-percent.php"
headers = {"User-Agent": "Mozilla/5.0"}

response = requests.get(url, headers=headers)
if response.status_code != 200:
    print(f"❌ Failed to load page: {response.status_code}")
    exit(1)

soup = BeautifulSoup(response.text, "html.parser")
table = soup.find("table", class_="tablesorter")

if not table:
    print("❌ Could not find data table on the page.")
    exit(1)

# === STEP 2: Extract headers and data ===
headers = [th.get_text(strip=True) for th in table.find("thead").find_all("th")]
rows = []

for tr in table.find("tbody").find_all("tr"):
    cols = [td.get_text(strip=True) for td in tr.find_all("td")]
    if cols:
        row = dict(zip(headers, cols))
        rows.append(row)

if not rows:
    print("⚠️ No rows found in table body.")
    exit(1)

print(f"✅ Scraped {len(rows)} players from Rotowire.")

# === STEP 3: Save to CSV ===
csv_file = FILENAME
with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)

print(f"💾 CSV written locally: {csv_file}")

# === STEP 4: Upload to S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(csv_file, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === STEP 5: Clean up local file ===
os.remove(csv_file)
print(f"🧹 Cleaned up local file: {csv_file}")
