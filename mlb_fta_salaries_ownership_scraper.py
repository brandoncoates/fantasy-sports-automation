import os
import time
import csv
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime

# URLs
URLS = {
    "DraftKings": "https://www.rotowire.com/daily/mlb/player-roster-percent.php?site=DraftKings",
    "FanDuel": "https://www.rotowire.com/daily/mlb/player-roster-percent.php?site=FanDuel"
}

# Setup headless Chrome
def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    return webdriver.Chrome(options=chrome_options)

# Scrape data from a given site
def scrape_site(site, url):
    driver = get_driver()
    driver.get(url)
    time.sleep(5)  # Let page load fully

    rows = driver.find_elements(By.XPATH, '//table[contains(@class, "tablesorter")]/tbody/tr')
    data = []
    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) >= 5:
            player = cols[0].text.strip()
            team = cols[1].text.strip()
            opponent = cols[2].text.strip()
            salary = cols[3].text.strip()
            roster_pct = cols[4].text.strip()
            data.append([player, team, opponent, salary, roster_pct, site])
    
    driver.quit()
    return data

# Save data to CSV
def save_to_csv(data, filename):
    headers = ["Player", "Team", "Opponent", "Salary", "Roster%", "Site"]
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

# Upload CSV to S3
def upload_to_s3(local_path, bucket_name, s3_path):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    s3.upload_file(local_path, bucket_name, s3_path)

# Main execution
def main():
    all_data = []
    for site, url in URLS.items():
        print(f"Scraping {site}...")
        all_data.extend(scrape_site(site, url))

    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"mlb_salaries_ownership_{today}.csv"
    local_path = f"/tmp/{filename}"
    s3_path = f"fantasy-baseball/salaries-ownership/{filename}"
    bucket = "your-s3-bucket-name"  # Replace with your bucket name

    save_to_csv(all_data, local_path)
    upload_to_s3(local_path, bucket, s3_path)
    print(f"Uploaded {filename} to S3 bucket {bucket}")

if __name__ == "__main__":
    main()
