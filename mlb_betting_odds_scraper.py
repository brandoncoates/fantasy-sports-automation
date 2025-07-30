#!/usr/bin/env python3
import os
import requests
import json
from datetime import datetime
import boto3
import re

# === CONFIG ===
API_KEY    = os.getenv("ODDS_API_KEY", "your_api_key_here")
REGION     = os.getenv("AWS_REGION", "us-east-1")
BUCKET     = os.getenv("S3_BUCKET_NAME", "fantasy-sports-csvs")
S3_FOLDER  = "baseball/betting"
SPORT      = "baseball_mlb"
MARKETS    = "totals,spreads,team_totals"
BOOKMAKERS = "fanduel"

# === DATE + FILE SETUP ===
target_date = datetime.now().strftime("%Y-%m-%d")
output_dir  = "mlb_daily_odds"
os.makedirs(output_dir, exist_ok=True)
filename    = f"mlb_betting_odds_{target_date}.json"
local_path  = os.path.join(output_dir, filename)
s3_key      = f"{S3_FOLDER}/{filename}"

# === TEAM NAME NORMALIZATION ===
TEAM_NAME_MAP = {
    "dbacks": "Arizona Diamondbacks", "diamondbacks": "Arizona Diamondbacks",
    "braves": "Atlanta Braves", "orioles": "Baltimore Orioles",
    "redsox": "Boston Red Sox", "whitesox": "Chicago White Sox",
    "cubs": "Chicago Cubs", "reds": "Cincinnati Reds",
    "guardians": "Cleveland Guardians", "rockies": "Colorado Rockies",
    "tigers": "Detroit Tigers", "astros": "Houston Astros",
    "royals": "Kansas City Royals", "angels": "Los Angeles Angels",
    "dodgers": "Los Angeles Dodgers", "marlins": "Miami Marlins",
    "brewers": "Milwaukee Brewers", "twins": "Minnesota Twins",
    "mets": "New York Mets", "yankees": "New York Yankees",
    "athletics": "Oakland Athletics", "phillies": "Philadelphia Phillies",
    "pirates": "Pittsburgh Pirates", "padres": "San Diego Padres",
    "giants": "San Francisco Giants", "mariners": "Seattle Mariners",
    "cardinals": "St. Louis Cardinals", "rays": "Tampa Bay Rays",
    "rangers": "Texas Rangers", "bluejays": "Toronto Blue Jays",
    "nationals": "Washington Nationals"
}
def normalize(name): return re.sub(r"[ .'-]", "", name.lower())

# === FETCH ODDS ===
print(f"📡 Requesting MLB betting odds from FanDuel for {target_date}...")
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
games = resp.json()
if not games:
    print("⚠️ No odds returned — exiting.")
    exit(0)

# === PARSE RESULTS ===
results = []
for game in games:
    home = TEAM_NAME_MAP.get(normalize(game.get("home_team", "")), game.get("home_team", ""))
    away = TEAM_NAME_MAP.get(normalize(game.get("away_team", "")), game.get("away_team", ""))
    time_str = game.get("commence_time", "")[:19].replace("T", " ")

    for book in game.get("bookmakers", []):
        if book.get("title") != "FanDuel":
            continue

        over_under = None
        spread = None
        favorite = None
        underdog = None
        implied_totals = {}

        for market in book.get("markets", []):
            m_type = market.get("key")
            for outcome in market.get("outcomes", []):
                name = outcome.get("name", "")
                point = outcome.get("point")

                if m_type == "totals" and name.lower() == "over" and point:
                    over_under = point

                elif m_type == "spreads" and point is not None:
                    if name == home and point < 0:
                        favorite, underdog = home, away
                        spread = abs(point)
                    elif name == away and point < 0:
                        favorite, underdog = away, home
                        spread = abs(point)

                elif m_type == "team_totals" and point is not None:
                    canon = TEAM_NAME_MAP.get(normalize(name), name)
                    implied_totals[canon] = point

        results.append({
            "date": target_date,
            "time": time_str,
            "bookmaker": "FanDuel",
            "home_team": home,
            "away_team": away,
            "over_under": over_under,
            "spread": spread,
            "favorite": favorite,
            "underdog": underdog,
            "implied_totals": implied_totals
        })

# === SAVE LOCALLY ===
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)
print(f"💾 Saved betting odds to {local_path} ({len(results)} games)")

# === UPLOAD TO S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{s3_key}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(local_path, BUCKET, s3_key)
    print("✅ Upload successful.")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)
