import csv
import requests
import boto3
from datetime import datetime
import os
from bs4 import BeautifulSoup

# === CONFIG ===
REGION = "us-east-2"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/projections"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_fta_salaries_ownership_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

FTA_URL = "https://www.fantasyteamadvice.com/mlb-dfs-ownership/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

print("📡 Downloading FTA CSV...")
response = requests.get(FTA_URL, headers=HEADERS, verify=False)
soup = BeautifulSoup(response.content, "html.parser")

# === Detect tables ===
tables = soup.find_all("table")
print(f"🔍 Found {len(tables)} tables")

if not tables:
    raise Exception("❌ No tables found on the page. Page structure might have changed.")

# === Function to parse any table ===
def parse_table(table, source_label):
    data = []
    headers = [th.text.strip() for th in table.find_all("th")]
    headers.append("Source")

    for row in table.find_all("tr")[1:]:
        cells = row.find_all(["td", "th"])
        row_data = [cell.text.strip() for cell in cells]
        if row_data:
            row_data.append(source_label)
            data.append(dict(zip(headers, row_data)))
    return headers, data

all_data = []
all_headers = set()

# === Try parsing each table ===
for i, table in enumerate(tables):
    label = f"Table{i+1}"
    headers, data = parse_table(table, label)
    all_data.extend(data)
    all_headers.update(headers)

# === Write combined CSV ===
print("💾 Saving locally...")
with open(FILENAME, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(all_headers))
    writer.writeheader()
    for row in all_data:
        writer.writerow(row)

# === Upload to S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(FILENAME, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === Cleanup ===
os.remove(FILENAME)
print(f"🧹 Removed local file: {FILENAME}")
