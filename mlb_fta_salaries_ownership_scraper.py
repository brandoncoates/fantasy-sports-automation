import os
import csv
import boto3
import requests
from datetime import datetime
from bs4 import BeautifulSoup

URLS = {
    "DraftKings": "https://www.fantasyteamadvice.com/mlb-dfs-draftkings-ownership-projections/",
    "FanDuel": "https://www.fantasyteamadvice.com/mlb-dfs-fanduel-ownership-projections/"
}

def parse_fta_table(site, html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    data = []

    if not table:
        print(f"No table found for {site}")
        return data

    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue
        player = cols[0].get_text(strip=True)
        position = cols[1].get_text(strip=True)
        team = cols[2].get_text(strip=True)
        opponent = cols[3].get_text(strip=True)
        salary = cols[4].get_text(strip=True)
        ownership = cols[5].get_text(strip=True)
        data.append([player, position, team, opponent, salary, ownership, site])
    
    return data

def save_to_csv(data, filename):
    headers = ["Player", "Position", "Team", "Opponent", "Salary", "Ownership%", "Site"]
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
        try:
            response = requests.get(url, verify=False)
            if response.status_code == 200:
                site_data = parse_fta_table(site, response.text)
                print(f"{site}: {len(site_data)} rows scraped.")
                all_data.extend(site_data)
            else:
                print(f"Failed to fetch {site} data: Status {response.status_code}")
        except Exception as e:
            print(f"Error fetching {site}: {e}")

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
