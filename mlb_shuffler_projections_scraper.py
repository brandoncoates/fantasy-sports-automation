import os
import csv
import requests
import boto3
from datetime import datetime

# === CONFIG ===
REGION = "us-east-2"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/projections"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_shuffler_projections_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === HEADERS ===
API_KEY = os.environ.get("SHUFFLER_API_KEY")
if not API_KEY:
    raise ValueError("SHUFFLER_API_KEY environment variable not set")

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

# === Step 1: Get projections ===
print("📡 Fetching projections from Shuffler API...")
url = f"https://api.shuffler.io/v3/json/PlayerGameProjectionStatsByDate/{DATE}"
response = requests.get(url, headers=HEADERS)

if response.status_code != 200:
    print(f"❌ Failed to fetch data: {response.status_code} - {response.text}")
    exit(1)

data = response.json()
if not isinstance(data, list):
    print("❌ Unexpected data format:", type(data))
    exit(1)

# === Step 2: Write to CSV ===
print("💾 Saving CSV locally...")
with open(FILENAME, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

# === Step 3: Upload to S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(FILENAME, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === Step 4: Cleanup ===
os.remove(FILENAME)
print(f"🧹 Removed local file: {FILENAME}")
