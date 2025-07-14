import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
import os

# Define URLs
DK_URL = 'https://www.rotowire.com/daily/mlb/player-roster-percent.php?site=DraftKings'

# Today's date for the filename
today = datetime.datetime.now().strftime('%Y-%m-%d')
filename = f"mlb_fta_salaries_ownership_dk_{today}.csv"

def scrape_roster_percentages(url):
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }

    # Get HTML content
    response = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the table
    table = soup.find('table', {'id': 'player-table-datatable'})
    if table is None:
        raise Exception("❌ Table not found on the page.")

    # Parse table data
    df = pd.read_html(str(table))[0]
    df['Source'] = 'DraftKings'

    return df

# Scrape DraftKings
print("📥 Scraping DraftKings...")
df_dk = scrape_roster_percentages(DK_URL)

# Save locally
df_dk.to_csv(filename, index=False)
print(f"✅ CSV saved: {filename}")

# Optional: Upload to S3 if needed
# (add your boto3 code here if doing upload)

# Clean up
os.remove(filename)
print("🧹 Local CSV deleted.")
