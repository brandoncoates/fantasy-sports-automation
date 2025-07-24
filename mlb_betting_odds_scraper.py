import os
import requests
import json
from datetime import datetime
import boto3

# === CONFIG ===
API_KEY    = os.environ["ODDS_API_KEY"]      # or keep your hard‑coded key
REGION     = os.environ["AWS_REGION"]        # e.g. "us-east-1"
BUCKET     = os.environ["S3_BUCKET_NAME"]    # "fantasy-sports-csvs"
S3_FOLDER  = "baseball/betting"              # must match your S3 folder
SPORT      = "baseball_mlb"
MARKETS    = "totals,h2h,spreads"
BOOKMAKERS = "draftkings,fanduel,pointsbetus"

# Today's date
target_date = datetime.now().strftime("%Y-%m-%d")

# Prepare output
output_dir = "mlb_daily_odds"
os.makedirs(output_dir, exist_ok=True)
filename    = f"mlb_betting_odds_{target_date}.json"
local_path  = os.path.join(output_dir, filename)
s3_key      = f"{S3_FOLDER}/{filename}"

# Fetch odds
url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
params = {
    "apiKey":     API_KEY,
    "regions":    "us",
    "markets":    MARKETS,
    "bookmakers": BOOKMAKERS,
    "oddsFormat": "decimal",
    "dateFormat": "iso",
}

resp = requests.get(url, params=params)
resp.raise_for_status()
odds_json = resp.json()
if not odds_json:
    print(f"⚠️ No odds for {target_date}, exiting.")
    exit(0)

# Build flat list
odds_data = []
for game in odds_json:
    home = game.get("home_team",""); away = game.get("away_team","")
    tm   = game.get("commence_time","")[:19].replace("T"," ")
    for book in game.get("bookmakers",[]):
        bname = book.get("title","")
        for m in book.get("markets",[]):
            key = m.get("key","")
            for o in m.get("outcomes",[]):
                odds_data.append({
                    "date":       target_date,
                    "time":       tm,
                    "bookmaker":  bname,
                    "market":     key,
                    "home_team":  home,
                    "away_team":  away,
                    "team":       o.get("name",""),
                    "odds":       o.get("price",None),
                    "point":      o.get("point",None),
                })

# Save JSON locally
with open(local_path,"w",encoding="utf-8") as f:
    json.dump(odds_data,f,ensure_ascii=False,indent=2)
print(f"💾 Betting odds written: {local_path} ({len(odds_data)} rows)")

# Upload to S3
s3 = boto3.client("s3", region_name=REGION)
s3.upload_file(local_path, BUCKET, s3_key)
print(f"☁️ Uploaded to s3://{BUCKET}/{s3_key}")
