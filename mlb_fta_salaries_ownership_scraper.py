import os
import csv
import boto3
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# URLs to scrape
URLS = {
    "DraftKings": "https://www.rotowire.com/daily/mlb/player-roster-percent.php?site=DraftKings",
    "FanDuel": "https://www.rotowire.com/daily/mlb/player-roster-percent.php?site=FanDuel"
}

# Setup headless browser
def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920x1080")
    return webdriver.Chrome(options=chrome_options)

# Scrape data from a single site
def scrape_site(site, url):
    driver = get_driver()
    driver.get(url)

    wait = WebDriverWait(driver, 15)
    wait.until(EC.presence_of_element_located((By.XPATH, '//table[contains(@class, "tablesorter")]/tbody/tr')))
    
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

# Save to CSV
def save_to_csv(data, filename):
    headers = ["Player", "Team", "Opponent", "Salary", "Roster%", "Site"]
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

# Upload to S3
def upload_to_s3(local_path, bucket_name, s3_path):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    s3.upload_file(local_path, bucket_name, s3_path)

# Main function
def main():
    all_data = []
    for site, url in URLS.items():
        print(f"Scraping {site}...")
        site_data = scrape_site(site, url)
        print(f"Found {len(site_data)} rows for {site}")
        all_data.extend(site_data)

    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"mlb_salaries_ownership_{today}.csv"
    local_path = f"/tmp/{filename}"
    s3_path = f"baseball/salaries-ownership/{filename}"
    bucket = "fantasy-sports-csvs"

    save_to_csv(all_data, local_path)
    upload_to_s3(local_path, bucket, s3_path)
    print(f"Uploaded {filename} to S3 bucket '{bucket}' at path '{s3_path}'")

if __name__ == "__main__":
    main()
