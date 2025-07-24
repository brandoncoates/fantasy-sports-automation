# mlb_betting_odds_scraper.py

import os
import requests
import json
from datetime import datetime
import boto3

# === CONFIG ===
API_KEY    = os.getenv("ODDS_API_KEY", "32c95ea767253beab2da2d1563a9150e")
REGION     = os.getenv("AWS_REGION", "us-east-1")
# This will default to your real bucket if S3_BUCKET_NAME is missing or empty
BUCKET     = os.getenv("S3_BUCKET_NAME") or "fantasy-sports-csvs"
S3_FOLDER  = "baseball/betting"

SPORT      = "baseball_mlb"
MARKETS    = "totals,h2h,spreads"
BOOKMAKERS = "draftkings,fanduel,pointsbetus"

# Today's date
target_date = datetime.now().strftime("%Y-%m-%d")

# Prepare paths
output_dir  = "mlb_daily_odds"
os.makedirs(output_dir, exist_ok=True)
filename    = f"mlb_betting_odds_{target_date}.json"
local_path  = os.path.join(output_dir, filename)
s3_key      = f"{S3_FOLDER}/{filename}"

# === DEBUG: print out what we’ve got ===
print("🔍 DEBUG:")
print("  ODDS_API_KEY    :", "******" if API_KEY else "(empty)")
print("  AWS_REGION      :", REGION)
print("  S3_BUCKET_NAME  :", os.environ.get("S3_BUCKET_NAME"))
print("  Resolved BUCKET :", BUCKET)
print("  S3 Key          :", s3_key)

# === FETCH ODDS ===
print(f"📡 Requesting MLB betting odds for {target_date}…")
resp = requests.get(
    f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds",
    params={
        "apiKey":     API_KEY,
        "regions":    "us",
        "markets":    MARKETS,
        "bookmakers": BOOKMAKERS,
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    },
    timeout=10
)
resp.raise_for_status()
odds_json = resp.json()
if not odds_json:
    print(f"⚠️ No betting odds returned for {target_date}. Exiting.")
    exit(0)

# === PARSE INTO FLAT LIST ===
odds_data = []
for game in odds_json:
    home = game.get("home_team","")
    away = game.get("away_team","")
    tm   = game.get("commence_time","")[:19].replace("T"," ")
    for book in game.get("bookmakers",[]):
        bname = book.get("title","")
        for m in book.get("markets",[]):
            mkey = m.get("key","")
            for o in m.get("outcomes",[]):
                odds_data.append({
                    "date":       target_date,
                    "time":       tm,
                    "bookmaker":  bname,
                    "market":     mkey,
                    "home_team":  home,
                    "away_team":  away,
                    "team":       o.get("name",""),
                    "odds":       o.get("price",None),
                    "point":      o.get("point",None),
                })

# === SAVE TO JSON LOCALLY ===
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(odds_data, f, ensure_ascii=False, indent=2)
print(f"💾 Betting odds written locally: {local_path} ({len(odds_data)} entries)")

# === UPLOAD TO S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{s3_key}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(local_path, BUCKET, s3_key)
    print(f"✅ Uploaded to s3://{BUCKET}/{s3_key}")
except Exception as e:
    print(f"❌ S3 upload failed: {e}")
    exit(1)
