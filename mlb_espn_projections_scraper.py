import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import boto3
from io import StringIO

# === CONFIG ===
base_url = "https://www.espn.com"
fantasy_home = f"{base_url}/fantasy/baseball/"
target_date = datetime.now().strftime('%Y-%m-%d')
filename = f"mlb_espn_projections_{target_date}.csv"

# === S3 CONFIG ===
bucket_name = "fantasy-sports-csvs"
s3_folder = "baseball/playerprojections"
s3_key = f"{s3_folder}/{filename}"

# === Step 1: Load Fantasy Baseball homepage ===
headers = {"User-Agent": "Mozilla/5.0"}
home_response = requests.get(fantasy_home, headers=headers)
home_soup = BeautifulSoup(home_response.text, "html.parser")

# === Step 2: Find the most recent "hitter projections" article ===
article_url = None
for link in home_soup.find_all("a", href=True):
    href = link['href']
    text = link.get_text(strip=True).lower()
    if "hitter projections" in href or "hitter projections" in text:
        article_url = href
        if not article_url.startswith("http"):
            article_url = base_url + article_url
        break

if not article_url:
    print("❌ Could not find ESPN hitter projections article.")
    exit()

print(f"🔗 Found article: {article_url}")

# === Step 3: Load article and look for tables ===
article_response = requests.get(article_url, headers=headers)
article_soup = BeautifulSoup(article_response.text, "html.parser")
tables = article_soup.find_all("table")

if not tables:
    print("❌ No tables found in the article.")
    exit()

# === Step 4: Parse the first table as DataFrame ===
try:
    df = pd.read_html(str(tables[0]))[0]
    df.columns = [col.strip() for col in df.columns]  # Clean headers
except Exception as e:
    print(f"❌ Error reading table: {e}")
    exit()

# === Step 5: Convert DataFrame to CSV in memory ===
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

# === Step 6: Upload CSV to S3 ===
s3 = boto3.client("s3")
s3.put_object(
    Bucket=bucket_name,
    Key=s3_key,
    Body=csv_buffer.getvalue()
)

print(f"☁️ Uploaded directly to S3: s3://{bucket_name}/{s3_key} ({len(df)} rows)")
