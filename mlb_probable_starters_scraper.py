import os
import requests
import json
from datetime import datetime
import boto3

from shared.normalize_name import normalize_name

# === CONFIG ===
REGION    = os.environ.get("AWS_REGION", "us-east-1")
# If the env var is unset or empty, fall back to your real bucket name
BUCKET    = os.environ.get("S3_BUCKET_NAME") or "fantasy-sports-csvs"
S3_FOLDER = "baseball/probablestarters"

DATE      = datetime.now().strftime("%Y-%m-%d")
filename  = f"mlb_probable_starters_{DATE}.json"
s3_key    = f"{S3_FOLDER}/{filename}"

# === GET SCHEDULE & PROBABLE PITCHERS ===
url = (
    f"https://statsapi.mlb.com/api/v1/schedule"
    f"?sportId=1&date={DATE}&hydrate=probablePitcher"
)
print(f"📡 Requesting probable starters for {DATE}…")
resp = requests.get(url)
if resp.status_code != 200:
    print(f"❌ API error {resp.status_code}: {resp.text}")
    exit(1)

data = resp.json()
dates = data.get("dates", [])
if not dates or not dates[0].get("games"):
    print(f"⚠️ No MLB games scheduled for {DATE}. Exiting.")
    exit(0)

games = dates[0]["games"]

def get_throw_hand(player_id):
    if not player_id:
        return ""
    try:
        info = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{player_id}"
        ).json()["people"][0]
        return info.get("pitchHand", {}).get("code", "")
    except:
        return ""

# === BUILD RECORDS ===
records = []
for game in games:
    gid      = game["gamePk"]
    home     = game["teams"]["home"]
    away     = game["teams"]["away"]
    home_tm  = home["team"]["name"]
    away_tm  = away["team"]["name"]

    raw_home = home.get("probablePitcher", {}).get("fullName", "")
    raw_away = away.get("probablePitcher", {}).get("fullName", "")
    home_nm  = normalize_name(raw_home)
    away_nm  = normalize_name(raw_away)

    home_id  = home.get("probablePitcher", {}).get("id")
    away_id  = away.get("probablePitcher", {}).get("id")
    home_hd  = get_throw_hand(home_id)
    away_hd  = get_throw_hand(away_id)

    records.append({
        "date":             DATE,
        "game_id":          gid,
        "away_team":        away_tm,
        "away_pitcher":     away_nm,
        "away_throw_hand":  away_hd,
        "home_team":        home_tm,
        "home_pitcher":     home_nm,
        "home_throw_hand":  home_hd
    })

print(f"✅ Found {len(records)} probable starters for {DATE}")

# === SAVE JSON LOCALLY ===
os.makedirs("mlb_probable_starters", exist_ok=True)
local_path = os.path.join("mlb_probable_starters", filename)
with open(local_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)
print(f"💾 JSON written locally: {local_path}")

# === UPLOAD JSON TO S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{s3_key}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(local_path, BUCKET, s3_key)
    print(f"✅ Upload complete: s3://{BUCKET}/{s3_key}")
except Exception as e:
    print(f"❌ S3 upload failed: {e}")
    exit(1)
