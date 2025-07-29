import os
import json
import requests
from datetime import datetime
import boto3

# import the normalization function
from shared.normalize_name import normalize_name

# === CONFIG ===
REGION     = "us-east-1"
BUCKET     = "fantasy-sports-csvs"
S3_FOLDER  = "baseball/rosters"
DATE       = datetime.now().strftime("%Y-%m-%d")
filename   = f"mlb_rosters_{DATE}.json"
S3_KEY     = f"{S3_FOLDER}/{filename}"
TEAMS_URL  = "https://statsapi.mlb.com/api/v1/teams?sportId=1"
PROBABLES  = f"baseball/probablestarters/mlb_probable_starters_{DATE}.json"

# === GET MLB TEAMS ===
print("📡 Getting MLB teams...")
teams_response = requests.get(TEAMS_URL)
teams = teams_response.json().get("teams", [])

records = []

for team in teams:
    team_id = team.get("id")
    team_name = team.get("name")

    print(f"🔍 Getting active roster for {team_name}...")
    roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active"
    roster_response = requests.get(roster_url)

    if roster_response.status_code != 200:
        print(f"❌ Failed to get roster for {team_name}: {roster_response.text}")
        continue

    for player in roster_response.json().get("roster", []):
        person = player.get("person", {})
        raw_name = person.get("fullName", "")
        full_name = normalize_name(raw_name)
        player_id = person.get("id", "")
        position = player.get("position", {}).get("abbreviation", "")
        status = player.get("status", {})
        status_code = status.get("code", "")
        status_desc = status.get("description", "")

        # Fetch player details for hand info
        details_url = f"https://statsapi.mlb.com/api/v1/people/{player_id}"
        details_response = requests.get(details_url)

        bats = throws = None
        if details_response.status_code == 200:
            details = details_response.json().get("people", [{}])[0]
            bats = details.get("batSide", {}).get("code", "")
            throws = details.get("pitchHand", {}).get("code", "")

        records.append({
            "date": DATE,
            "team": team_name,
            "player": full_name,
            "player_id": player_id,
            "position": position,
            "status_code": status_code,
            "status_description": status_desc,
            "bats": bats,
            "throws": throws
        })

# === FALLBACK: ADD MISSING PROBABLE STARTERS ===
try:
    with open(PROBABLES, "r", encoding="utf-8") as f:
        starters = json.load(f)
except Exception as e:
    print(f"⚠️ Could not load probable starters: {e}")
    starters = []

# Build set of names already included
roster_names = {rec["player"].strip().lower() for rec in records}

for game in starters:
    for role in ["home_pitcher", "away_pitcher"]:
        name = game.get(role, "").strip()
        if not name or name.lower() in roster_names:
            continue  # already in list

        # Guess team name from game context
        team = game.get("home_team") if role == "home_pitcher" else game.get("away_team")

        print(f"➕ Injecting missing probable starter: {name}")
        records.append({
            "date": DATE,
            "team": team,
            "player": name,
            "player_id": f"manual-{normalize_name(name)}",
            "position": "P",
            "status_code": "A",
            "status_description": "Probable Starter (Injected)",
            "bats": "R",
            "throws": "R"
        })

print(f"✅ Final roster count: {len(records)} players.")

# === SAVE TO JSON ===
output_dir = "mlb_rosters"
os.makedirs(output_dir, exist_ok=True)
local_path = os.path.join(output_dir, filename)

with open(local_path, mode="w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"💾 JSON written locally: {local_path}")

# === UPLOAD TO S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(local_path, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)
