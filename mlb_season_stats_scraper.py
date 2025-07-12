import os
import pandas as pd
import requests
from datetime import datetime

# === CONFIG ===
season = datetime.now().year
output_dir = "mlb_season_stats"
os.makedirs(output_dir, exist_ok=True)
filename = f"mlb_season_stats_{season}.csv"
output_path = os.path.join(output_dir, filename)

# === MLB API Endpoint (Batting stats across entire league) ===
url = (
    f"https://statsapi.mlb.com/api/v1/stats"
    f"?stats=season&group=hitting&gameType=R&season={season}&limit=10000"
)

response = requests.get(url)
if response.status_code != 200:
    print(f"❌ Failed to fetch MLB season stats. Status code: {response.status_code}")
    exit()

data = response.json().get("stats", [])[0].get("splits", [])

# === Convert to flat table ===
players = []
for player in data:
    stat = player.get("stat", {})
    player_info = player.get("player", {})
    team_info = player.get("team", {})

    players.append({
        "Player": player_info.get("fullName"),
        "Team": team_info.get("name"),
        "Position": player_info.get("primaryPosition", {}).get("abbreviation", ""),
        "Games Played": stat.get("gamesPlayed"),
        "Plate Appearances": stat.get("plateAppearances"),
        "At Bats": stat.get("atBats"),
        "Runs": stat.get("runs"),
        "Hits": stat.get("hits"),
        "Doubles": stat.get("doubles"),
        "Triples": stat.get("triples"),
        "Home Runs": stat.get("homeRuns"),
        "RBIs": stat.get("rbi"),
        "Stolen Bases": stat.get("stolenBases"),
        "Walks": stat.get("baseOnBalls"),
        "Strikeouts": stat.get("strikeOuts"),
        "AVG": stat.get("avg"),
        "OBP": stat.get("obp"),
        "SLG": stat.get("slg"),
        "OPS": stat.get("ops"),
    })

# === Save to CSV ===
df = pd.DataFrame(players)
df.to_csv(output_path, index=False)
print(f"✅ Saved {len(df)} player rows to {output_path}")
