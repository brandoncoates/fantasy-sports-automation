import os
import csv
import boto3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Example props URL (adjust if this changes)
URL = "https://www.numberfire.com/mlb/daily-fantasy/daily-baseball-projections"

def scrape_numberfire_props():
    response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code != 200:
        print(f"Failed to fetch page: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"class": "projection-table"})

    if not table:
        print("⚠️ Could not find props table.")
        return []

    rows = table.find_all("tr")[1:]  # Skip header
    data = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 10:
            continue

        player = cols[0].get_text(strip=True)
        team = cols[1].get_text(strip=True)
        hits = cols[5].get_text(strip=True)
        hr = cols[6].get_text(strip=True)
        rbi = cols[7].get_text(strip=True)
        strikeouts = cols[8].get_text(strip=True)

        # Fake odds values since NumberFire shows projections, not betting lines
        fake_odds = "-"

        data.append([player, team, "Hits", hits, fake_odds, fake_odds])
        data.append([player, team, "Home Runs", hr, fake_odds, fake_odds])
        data.append([player, team, "RBIs", rbi, fake_odds, fake_odds])
        data.append([player, team, "Pitcher Strikeouts", strikeouts, fake_odds, fake_odds])

    return data

def save_to_csv(data, filename):
    headers = ["Player", "Team", "Prop Type", "Line", "Over Odds", "Under Odds"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
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
    data = scrape_numberfire_props()
    print(f"✅ Total props scraped: {len(data)}")

    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"numberfire_props_{today}.csv"
    local_path = f"/tmp/{filename}"
    s3_path = f"baseball/props-numberfire/{filename}"
    bucket = "fantasy-sports-csvs"

    save_to_csv(data, local_path)
    upload_to_s3(local_path, bucket, s3_path)
    print(f"✅ Uploaded to S3: {s3_path}")

if __name__ == "__main__":
    main()
