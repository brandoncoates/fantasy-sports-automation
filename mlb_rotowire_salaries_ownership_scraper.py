from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import boto3
import os
import time

# Define base URL and platforms
ROTOWIRE_URL = "https://www.rotowire.com/daily/mlb/player-roster-percent.php?site={site}"
PLATFORMS = ["DraftKings", "FanDuel"]

# S3 info
bucket_name = "fantasy-sports-csvs"
s3_folder = "baseball/rotowire-salaries"

def get_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options = webdriver.ChromeOptions()
    options.binary_location = "/usr/bin/chromium-browser"

    return driver

def scrape_rotowire_props(site):
    url = ROTOWIRE_URL.format(site=site)
    driver = get_driver()
    print(f"Loading {url}")
    driver.get(url)

    time.sleep(5)  # Wait for JavaScript to load content

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    table = soup.find("table", {"class": "player-table"})
    if not table:
        print(f"❌ No data table found for {site}")
        return None

    rows = table.find_all("tr")
    data = []
    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue
        player = cols[0].text.strip()
        team = cols[1].text.strip()
        pos = cols[2].text.strip()
        salary = cols[3].text.strip().replace("$", "").replace(",", "")
        roster_pct = cols[5].text.strip().replace("%", "")
        data.append({
            "Player": player,
            "Team": team,
            "Position": pos,
            "Salary": int(salary) if salary.isdigit() else None,
            "Roster %": float(roster_pct) if roster_pct.replace(".", "").isdigit() else None
        })

    return pd.DataFrame(data)

def upload_to_s3(df, site):
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{site.lower()}_salaries_ownership_{date_str}.csv"
    local_path = f"/tmp/{filename}"
    df.to_csv(local_path, index=False)

    s3_path = f"{s3_folder}/{filename}"
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket_name, s3_path)
    print(f"✅ Uploaded to S3: {s3_path}")

def main():
    for site in PLATFORMS:
        print(f"Scraping {site} data...")
        df = scrape_rotowire_props(site)
        if df is not None and not df.empty:
            print(f"✅ Scraped {len(df)} rows for {site}")
            upload_to_s3(df, site)
        else:
            print(f"❌ No data scraped for {site}")

if __name__ == "__main__":
    main()
