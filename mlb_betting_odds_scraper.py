import os
import requests
import json
from datetime import datetime
import boto3
import re

# === CONFIG ===
API_KEY    = os.getenv("ODDS_API_KEY", "your_api_key_here")
REGION     = os.getenv("AWS_REGION", "us-east-1")
BUCKET     = os.getenv("S3_BUCKET_NAME") or "fantasy-sports-csvs"
S3_FOLDER  = "baseball/betting"
SPORT      = "baseball_mlb"
MARKETS    = "totals,h2h,spreads"
BOOKMAKERS = "draftkings,fanduel,pointsbetus"

# Today's date
target_date = datetime.now().strftime("%Y-%m-%d")
output_dir  = "mlb_daily_odds"
os.makedirs(output_dir, exist_ok=True)
filename    = f"mlb_betting_odds_{target_date}.json"
local_path  = os.path.join(output_dir, filename)
s3_key      = f"{S3_FOLDER}/{filename}"

# === Normalize team names ===
TEAM_NAME_MAP = {
    "dbacks": "Arizona Diamondbacks",
    "diamondbacks": "Arizona Diamondbacks",
    "braves": "Atlanta Braves",
    "orioles": "Baltimore Orioles",
    "redsox": "Boston Red Sox",
    "whitesox": "Chicago White Sox",
    "cubs": "Chicago Cubs",
    "reds": "Cincinnati Reds",
    "guardians": "Cleveland Guardians",
    "rockies": "Colorado Rockies",
    "tigers": "Detroit Tigers",
    "astros": "Houston Astros",
    "royals": "Kansas City Royals",
    "angels": "Los Angeles Angels",
    "dodgers": "Los Angeles Dodgers",
    "marlins": "Miami Marlins",
    "brewers": "Milwaukee Brewers",
    "twins": "Minnesota Twins",
    "mets": "New York Mets",
    "yankees": "New York Yankees",
    "athletics": "Oakland Athletics",
    "phillies": "Philadelphia Phillies",
    "pirates": "Pittsburgh Pirates",
    "padres": "San Diego Padres",
    "giants": "San Francisco Giants",
    "mariners": "Seattle Mariners",
    "cardinals": "St. Louis Cardinals",
    "rays": "Tampa Bay Rays",
    "rangers": "Texas Rangers",
    "bluejays": "Toronto Blue Jays",
    "nationals": "Washington Nationals"
}

def normalize(name):
    return re.sub(r"[ .'-]", "", name.lower())

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

# === FLATTENED RESULTS ===
results = []
for game in odds_json:
    home = game.get("home_team", "")
    away = game.get("away_team", "")
    timestamp = game.get("commence_time", "")[:19].replace("T", " ")

    home_canon = TEAM_NAME_MAP.get(normalize(home), home)
    away_canon = TEAM_NAME_MAP.get(normalize(away), away)

    for book in game.get("bookmakers", []):
        bookmaker = book.get("title", "")
        over_under_total = None

        # First extract game total
        for market in book.get("markets", []):
            if market.get("key") == "totals":
                for o in market.get("outcomes", []):
                    if o.get("name", "").lower() == "over" and o.get("point") is not None:
                        over_under_total = o.get("point")

        # Store all market-level outcomes
        for market in book.get("markets", []):
            market_type = market.get("key")
            for o in market.get("outcomes", []):
                team_raw = o.get("name", "")
                canon_team = TEAM_NAME_MAP.get(normalize(team_raw), team_raw)
                results.append({
                    "date":       target_date,
                    "time":       timestamp,
                    "bookmaker":  bookmaker,
                    "market":     market_type,
                    "home_team":  home_canon,
                    "away_team":  away_canon,
                    "team":       canon_team,
                    "odds":       o.get("price"),
                    "point":      o.get("point"),
                    "over_under": over_under_total
                })

# === WRITE LOCALLY ===
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print(f"💾 Betting odds written locally: {local_path} ({len(results)} entries)")

# === UPLOAD TO S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{s3_key}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(local_path, BUCKET, s3_key)
    print(f"✅ Uploaded to s3://{BUCKET}/{s3_key}")
except Exception as e:
    print(f"❌ S3 upload failed: {e}")
    exit(1)
