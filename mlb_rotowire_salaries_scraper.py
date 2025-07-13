import os
import csv
import requests
import boto3
from datetime import datetime
from bs4 import BeautifulSoup

# === CONFIG ===
REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
DATE = datetime.now().strftime("%Y-%m-%d")
BASE_URL = "https://www.rotowire.com/daily/mlb/optimizer.php"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# === S3 PATHS ===
S3_PATHS = {
    "FanDuel": {
        "folder": "baseball/salaries/fanduel",
        "filename": f"mlb_fanduel_salaries_{DATE}.csv"
    },
    "DraftKings": {
        "folder": "baseball/salaries/draftkings",
        "filename": f"mlb_draftkings_salaries_{DATE}.csv"
    }
}

# === STEP 1: Request Optimizer Page ===
print("📡 Fetching Rotowire Optimizer page...")
response = requests.get(BASE_URL, headers=HEADERS)
if response.status_code != 200:
    print(f"❌ Failed to fetch page: {response.status_code}")
    exit(1)

soup = BeautifulSoup(response.text, "html.parser")
tables = soup.find_all("table", class_="optimals")

# === STEP 2: Identify Tables ===
site_tables = {}
for table in tables:
    heading = table.find_previous("h3")
    if heading:
        if "FanDuel" in heading.text:
            site_tables["FanDuel"] = table
        elif "DraftKings" in heading.text:
            site_tables["DraftKings"] = table

if not site_tables:
    print("❌ Could not find salary tables for FanDuel or DraftKings.")
    exit(1)

# === STEP 3: Parse Tables ===
def parse_table(table):
    headers = [th.text.strip() for th in table.find("thead").find_all("th")]
    rows = []
    for tr in table.find("tbody").find_all("tr"):
        cells = [td.text.strip() for td in tr.find_all("td")]
        if len(cells) == len(headers):
            rows.append(dict(zip(headers, cells)))
    return headers, rows

# === STEP 4: Process and Upload ===
s3 = boto3.client("s3", region_name=REGION)

for site, table in site_tables.items():
    headers, rows = parse_table(table)
    filename = S3_PATHS[site]["filename"]
    s3_key = f"{S3_PATHS[site]['folder']}/{filename}"

    print(f"💾 Writing {site} salary data to: {filename}")
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print(f"☁️ Uploading to s3://{BUCKET}/{s3_key}")
    try:
        s3.upload_file(filename, BUCKET, s3_key)
        print(f"✅ Upload complete: s3://{BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        exit(1)

    os.remove(filename)
    print(f"🧹 Cleaned up local file: {filename}")
