import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import boto3

# Define base URL and platforms
ROTOWIRE_URL = "https://www.rotowire.com/daily/mlb/player-roster-percent.php?site={site}"
PLATFORMS = ["DraftKings", "FanDuel"]

# S3 bucket info
bucket_name = "fantasy-sports-csvs"
s3_folder = "baseball/rotowire-salaries"

# Headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

def scrape_rotowire_props(site):
    url = ROTOWIRE_URL.format(site=site)
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to fetch data for {site}: {response.status_code}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", {"class": "player-table"})
    if not table:
        print(f"No data table found for {site}")
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
            print(f"Scraped {len(df)} rows for {site}")
            upload_to_s3(df, site)
        else:
            print(f"❌ No data scraped for {site}")

if __name__ == "__main__":
    main()
