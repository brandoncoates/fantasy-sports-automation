import os
import csv
import requests
import boto3
from datetime import datetime
from bs4 import BeautifulSoup

# === CONFIG ===
REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/salaries"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_fanduel_salaries_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === Step 1: Load FanDuel main MLB contests page ===
print("📡 Accessing FanDuel MLB contests...")
headers = {"User-Agent": "Mozilla/5.0"}
url = "https://www.fanduel.com/games/MLB"
response = requests.get(url, headers=headers)

if response.status_code != 200:
    print(f"❌ Error loading FanDuel page: {response.status_code}")
    exit(1)

soup = BeautifulSoup(response.text, "html.parser")

# === Step 2: Find the salary CSV link ===
csv_url = None
for link in soup.find_all("a", href=True):
    href = link["href"]
    if "salaryExport" in href and href.endswith(".csv"):
        csv_url = href
        break

if not csv_url:
    print("❌ Could not find FanDuel salary CSV link.")
    exit(1)

print(f"🔗 Found CSV: {csv_url}")

# === Step 3: Download CSV ===
csv_response = requests.get(csv_url, headers=headers)
if csv_response.status_code != 200:
    print(f"❌ Failed to download salary CSV: {csv_response.status_code}")
    exit(1)

csv_file = FILENAME
with open(csv_file, "w", newline="", encoding="utf-8") as f:
    f.write(csv_response.text)

print(f"💾 CSV downloaded locally: {csv_file}")

# === Step 4: Upload to S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(csv_file, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === Step 5: Clean up ===
os.remove(csv_file)
print(f"🧹 Cleaned up local file: {csv_file}")
