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

# === URLs ===
FTA_URL = "https://www.fantasyteamadvice.com/mlb-dfs-ownership/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# === Step 1: Scrape the HTML content ===
print("📡 Downloading FTA CSV...")
response = requests.get(FTA_URL, headers=HEADERS, verify=False)
soup = BeautifulSoup(response.content, "html.parser")

# === Step 2: Find all tables and distinguish them ===
tables = soup.find_all("table")

if len(tables) < 2:
    raise Exception("Expected at least two tables for salaries and ownerships")

salary_table = tables[0]
ownership_table = tables[1]

# === Step 3: Parse table content ===
def parse_table(table, source_label):
    data = []
    headers = [th.text.strip() for th in table.find_all("th")]
    headers.append("Source")  # Add a label column

    for row in table.find_all("tr")[1:]:
        cells = row.find_all(["td", "th"])
        row_data = [cell.text.strip() for cell in cells]
        if row_data:
            row_data.append(source_label)
            data.append(dict(zip(headers, row_data)))
    return headers, data

salary_headers, salary_data = parse_table(salary_table, "Salary")
ownership_headers, ownership_data = parse_table(ownership_table, "Ownership")

# === Step 4: Merge and Write CSV ===
combined_headers = list(set(salary_headers + ownership_headers))
print("💾 Saving locally...")
with open(FILENAME, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=combined_headers)
    writer.writeheader()
    for row in salary_data + ownership_data:
        writer.writerow(row)

# === Step 5: Upload to S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(FILENAME, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === Step 6: Cleanup ===
os.remove(FILENAME)
print(f"🧹 Removed local file: {FILENAME}")
