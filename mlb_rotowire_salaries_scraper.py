import os
import csv
import boto3
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# === CONFIG ===
REGION = "us-east-2"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/salaries"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_rotowire_salaries_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"

# === Step 1: Set up headless browser ===
print("🧠 Launching headless browser...")
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

url = "https://www.rotowire.com/daily/mlb/player-roster-percent.php"
print(f"🌐 Fetching page: {url}")
driver.get(url)

try:
    # Wait up to 15 seconds for the table to load
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.med\\:hidden"))
    )
except Exception as e:
    print(f"❌ Table didn't load in time: {e}")
    driver.quit()
    exit(1)

# === Step 2: Parse page ===
soup = BeautifulSoup(driver.page_source, "html.parser")
driver.quit()

table = soup.find("table", {"class": "med:hidden"})
if not table:
    print("❌ Could not find data table on the page.")
    exit(1)

# === Step 3: Extract data ===
print("📊 Parsing salary and roster percent data...")
rows = table.find_all("tr")
data = []
for row in rows:
    cols = [col.get_text(strip=True) for col in row.find_all(["td", "th"])]
    if cols:
        data.append(cols)

# === Step 4: Save CSV ===
with open(FILENAME, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(data)

print(f"💾 CSV saved: {FILENAME}")

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
print(f"🧹 Cleaned up local file: {FILENAME}")
