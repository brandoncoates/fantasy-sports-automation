import os
import csv
import boto3
import requests
from datetime import datetime
from bs4 import BeautifulSoup

URL = "https://www.rotowire.com/betting/mlb/player-props.php"

def scrape_rotowire_props():
    response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code != 200:
        print(f"Failed to fetch page: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    data = []

    if not table:
        print("⚠️ Could not find props table.")
        return []

    rows = table.find_all("tr")[1:]  # skip header row

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue

        player = cols[0].text.strip()
        team = cols[1].text.strip()
        stat = cols[2].text.strip()
        prop_line = cols[3].text.strip()
        over_odds = cols[4].text.strip()
        under_odds = cols[5].text.strip()

        data.append([player, team, stat, prop_line, over_odds, under_odds])

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
    data = scrape_rotowire_props()
    print(f"✅ Scraped {len(data)} props from Rotowire.")

    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"rotowire_props_{today}.csv"
    local_path = f"/tmp/{filename}"
    s3_path = f"baseball/props-rotowire/{filename}"
    bucket = "fantasy-sports-csvs"

    save_to_csv(data, local_path)
    upload_to_s3(local_path, bucket, s3_path)
    print(f"✅ Uploaded to S3: {s3_path}")

if __name__ == "__main__":
    main()
