import os
import csv
import boto3
import requests
from datetime import datetime
from bs4 import BeautifulSoup

URLS = {
    "DraftKings": "https://www.rotowire.com/daily/mlb/player-roster-percent.php?site=DraftKings",
    "FanDuel": "https://www.rotowire.com/daily/mlb/player-roster-percent.php?site=FanDuel"
}

def parse_roster_table(site, html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    data = []

    if not table:
        return data

    rows = table.find("tbody").find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 5:
            player = cols[0].get_text(strip=True)
            team = cols[1].get_text(strip=True)
            opponent = cols[2].get_text(strip=True)
            salary = cols[3].get_text(strip=True)
            roster_pct = cols[4].get_text(strip=True)
            data.append([player, team, opponent, salary, roster_pct, site])
    return data

def save_to_csv(data, filename):
    headers = ["Player", "Team", "Opponent", "Salary", "Roster%", "Site"]
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

def upload_to_s3(local_path, bucket_name, s3_path):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    s3.upload_file(local_path, bucket_name, s3_path)

def main():
    all_data = []

    for site, url in URLS.items():
        print(f"Fetching {site} data...")
        response = requests.get(url)
        if response.status_code == 200:
            site_data = parse_roster_table(site, response.text)
            print(f"{site}: {len(site_data)} rows scraped.")
            all_data.extend(site_data)
        else:
            print(f"Failed to fetch {site} data: Status {response.status_code}")

    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"mlb_salaries_ownership_{today}.csv"
    local_path = f"/tmp/{filename}"
    s3_path = f"baseball/salaries-ownership/{filename}"
    bucket = "fantasy-sports-csvs"

    save_to_csv(all_data, local_path)
    upload_to_s3(local_path, bucket, s3_path)
    print(f"Uploaded to S3: {s3_path}")

if __name__ == "__main__":
    main()
