import os
import csv
import requests
from datetime import datetime
import boto3

# === CONFIG ===
REGION = "us-east-1"
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/rosters"
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"mlb_rosters_{DATE}.csv"
S3_KEY = f"{S3_FOLDER}/{FILENAME}"
TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1"

# === GET MLB TEAMS ===
print("📡 Getting MLB teams...")
teams_response = requests.get(TEAMS_URL)
teams = teams_response.json().get("teams", [])

rows = []

for team in teams:
    team_id = team.get("id")
    team_name = team.get("name")

    print(f"🔍 Getting roster for {team_name}...")
    roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=fullRoster"
    roster_response = requests.get(roster_url)

    if roster_response.status_code != 200:
        print(f"❌ Failed to get roster for {team_name}: {roster_response.text}")
        continue

    roster = roster_response.json().get("roster", [])

    for player in roster:
        person = player.get("person", {})
        full_name = person.get("fullName", "")
        player_id = person.get("id", "")
        position = player.get("position", {}).get("abbreviation", "")
        status_code = player.get("status", {}).get("code", "")
        status_desc = player.get("status", {}).get("description", "")

        rows.append({
            "date": DATE,
            "team": team_name,
            "player": full_name,
            "player_id": player_id,
            "position": position,
            "status_code": status_code,
            "status_description": status_desc
        })

print(f"✅ Pulled {len(rows)} total players across {len(teams)} teams.")

# === SAVE TEMP FILE ===
csv_file = FILENAME
with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"💾 Temp file written locally: {csv_file}")

# === UPLOAD TO S3 ===
print(f"☁️ Uploading to s3://{BUCKET}/{S3_KEY}")
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.upload_file(csv_file, BUCKET, S3_KEY)
    print(f"✅ Upload complete: s3://{BUCKET}/{S3_KEY}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# === CLEANUP TEMP FILE ===
os.remove(csv_file)
print(f"🧹 Cleaned up local file {csv_file}")
