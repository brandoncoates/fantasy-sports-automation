import csv
import requests
import boto3
import os
from datetime import datetime

# === CONFIG ===
REGION = "us-east-2"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/salaries-ownership"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_fta_salaries_ownership_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === STEP 1: Download from FTA ===
print("📥 Downloading FTA CSV...")
FTA_URL = "https://www.fantasyteamadvice.com/mlb-dfs-ownership/"
response = requests.get(FTA_URL)

if response.status_code != 200:
    print(f"❌ Failed to download FTA CSV: {response.status_code}")
    exit(1)

with open(FILENAME, "wb") as f:
    f.write(response.content)

print("💾 Saved locally...")

# === STEP 2: Upload to S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(FILENAME, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === STEP 3: Cleanup ===
os.remove(FILENAME)
print(f"🧹 Removed local file: {FILENAME}")
