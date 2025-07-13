import os
import csv
import boto3
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# === CONFIG ===
REGION = "us-east-2"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/ownership"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_fta_ownership_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"
DEBUG_FILE = "fta_debug.html"

# === Step 1: Launch headless browser ===
print("🧠 Launching headless browser...")
options = Options()
options.add_argument("--headless=new")  # Use new headless mode for GitHub Actions
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=options)

# === Step 2: Navigate to page ===
url = "https://fantasyteamadvice.com/dfs/mlb/ownership"
print(f"🌐 Fetching page: {url}")
driver.get(url)

try:
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
    )
    print("✅ Table found!")
except Exception as e:
    print("❌ Table didn't load:", str(e))
    with open(DEBUG_FILE, "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    print(f"🛠️ Saved debug HTML to {DEBUG_FILE}")
    driver.quit()
    exit(1)

# === Step 3: Parse table with BeautifulSoup ===
soup = BeautifulSoup(driver.page_source, "html.parser")
driver.quit()

table = soup.find("table")
if not table:
    print("❌ Table not found.")
    exit(1)

print("📊 Extracting data...")
rows = table.find_all("tr")
data = []
for row in rows:
    cols = [col.get_text(strip=True) for col in row.find_all(["td", "th"])]
    if cols:
        data.append(cols)

# === Step 4: Save to CSV ===
with open(FILENAME, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(data)
print(f"💾 Saved CSV: {FILENAME}")

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
