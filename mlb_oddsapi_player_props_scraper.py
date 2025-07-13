import requests
import pandas as pd
from datetime import datetime
from io import StringIO
import boto3

# === CONFIG ===
API_KEY = '32c95ea767253beab2da2d1563a9150e'  # <-- Replace with your real key
SPORT = 'baseball_mlb'
REGION = 'us'
MARKETS = 'player_props'
ODDS_FORMAT = 'american'

# === S3 CONFIG ===
bucket_name = 'fantasy-sports-csvs'
s3_folder = 'baseball/playerprops'
target_date = datetime.now().strftime('%Y-%m-%d')
filename = f"mlb_oddsapi_player_props_{target_date}.csv"
s3_key = f"{s3_folder}/{filename}"

# === API Request ===
url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/players'
params = {
    'apiKey': API_KEY,
    'regions': REGION,
    'markets': MARKETS,
    'oddsFormat': ODDS_FORMAT,
}

print("📡 Requesting player props from The Odds API...")
response = requests.get(url, params=params)

if response.status_code != 200:
    print(f"❌ Failed to fetch props: {response.status_code} - {response.text}")
    exit()

props_json = response.json()
print(f"📊 Received {len(props_json)} player prop events")

if not props_json:
    print("⚠️ No props returned from Odds API. Exiting.")
    exit()

# === Parse player props ===
props_data = []

for event in props_json:
    player = event.get('player', {}).get('name', '')
    team = event.get('team', {}).get('name', '')
    opponent = event.get('opponent', {}).get('name', '')
    date = event.get('commence_time', '')[:10]

    for bookmaker in event.get('bookmakers', []):
        book = bookmaker.get('title', '')
        for market in bookmaker.get('markets', []):
            market_type = market.get('key', '')
            for outcome in market.get('outcomes', []):
                props_data.append({
                    'Date': date,
                    'Bookmaker': book,
                    'Player': player,
                    'Team': team,
                    'Opponent': opponent,
                    'Market': market_type,
                    'Prop': outcome.get('name', ''),
                    'Line': outcome.get('point', ''),
                    'Odds': outcome.get('price', '')
                })

# === Convert to CSV and upload to S3 ===
df = pd.DataFrame(props_data)

print(f"✅ Prepared {len(df)} prop rows for upload")

if df.empty:
    print("❌ DataFrame is empty. No file will be uploaded.")
    exit()

# Preview first few rows
print(df.head(3).to_string(index=False))

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

s3 = boto3.client('s3')
s3.put_object(
    Bucket=bucket_name,
    Key=s3_key,
    Body=csv_buffer.getvalue()
)

print(f"☁️ Upload complete: s3://{bucket_name}/{s3_key}")
