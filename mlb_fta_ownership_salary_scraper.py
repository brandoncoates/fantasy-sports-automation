import csv
import requests
import boto3
import os
from datetime import datetime

# === CONFIG ===
REGION = "us-east-2"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/projections"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_fta_projections_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"
URL = "https://fantasyteamadvice.com/wp-content/uploads/mlb-player-salaries.csv"

# === Step 1: Download CSV ===
print("📡 Downloading FTA CSV...")
response = requests.get(URL)
if response.status_code != 200:
    print(f"❌ Failed to fetch file: {response.status_code}")
    exit(1)

# === Step 2: Save locally ===
print("💾 Saving locally...")
with open(FILENAME, "wb") as f:
    f.write(response.content)

# === Step 3: Upload to S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(FILENAME, BUCKET, S3_KEY)
    print("✅ Upload complete.")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === Step 4: Cleanup ===
print(f"🧹 Deleting local file {FILENAME}")
os.remove(FILENAME)
