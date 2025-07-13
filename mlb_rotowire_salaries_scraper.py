import os
import csv
import requests
import boto3
from bs4 import BeautifulSoup
from datetime import datetime

# === CONFIG ===
REGION = "us-east-2"  # Updated from us-eat-1
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/salaries"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_rotowire_salaries_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"
URL = "https://www.rotowire.com/daily/mlb/player-roster-percent.php"

# === FETCH PAGE ===
print("📡 Fetching Rotowire Salary + Roster % page...")
headers = {
    "User-Agent": "Mozilla/5.0"
}
response = requests.get(URL, headers=headers)
if response.status_code != 200:
    print(f"❌ Error loading page: {response.status_code}")
    exit(1)

# === PARSE TABLE ===
soup = BeautifulSoup(response.text, "html.parser")
table = soup.find("table", class_="player-table sortable stats-table")
if not table:
    print("❌ Could not find data table on the page.")
    exit(1)

rows = []
tbody = table.find("tbody")
for tr in tbody.find_all("tr"):
    tds = tr.find_all("td")
    if len(tds) < 5:
        continue

    player = tds[0].get_text(strip=True)
    opponent = tds[1].get_text(strip=True)
    fd_salary = tds[2].get_text(strip=True).replace("$", "").replace(",", "")
    dk_salary = tds[3].get_text(strip=True).replace("$", "").replace(",", "")
    roster_pct = tds[4].get_text(strip=True).replace("%", "")

    rows.append({
        "date": DATE,
        "player": player,
        "opponent": opponent,
        "fanduel_salary": fd_salary,
        "draftkings_salary": dk_salary,
        "roster_percent": roster_pct
    })

print(f"✅ Parsed {len(rows)} player rows")

# === SAVE TO CSV ===
csv_file = FILENAME
with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"💾 Saved CSV locally: {csv_file}")

# === UPLOAD TO S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(csv_file, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === CLEAN UP ===
os.remove(csv_file)
print(f"🧹 Removed local file: {csv_file}")
