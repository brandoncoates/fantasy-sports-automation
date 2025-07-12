import requests
import csv
from datetime import datetime
import os

# Get today's date
today = datetime.now().strftime('%Y-%m-%d')

# FanDuel MLB salary CSV URL
url = "https://www.fanduel.com/api/game/get-slates?sport=mlb"

# Output file path
output_folder = "Fantasy Baseball/MLB Player Salaries"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, f"mlb_fanduel_salaries_{today}.csv")

try:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Filter classic slate
    classic_slate = None
    for slate in data.get("slates", []):
        if "Classic" in slate.get("gameType", "") and slate.get("isMultiDay", False) is False:
            classic_slate = slate
            break

    if not classic_slate:
        print("No classic slate found.")
        exit()

    players = classic_slate.get("fantasyPlayers", [])

    # Write CSV
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Name", "Team", "Opponent", "Position", "Salary", "FPPG", "Injury Status"
        ])
        for player in players:
            writer.writerow([
                player.get("firstName", "") + " " + player.get("lastName", ""),
                player.get("team"),
                player.get("opponent"),
                player.get("position"),
                player.get("salary"),
                player.get("fppg"),
                player.get("injuryStatus", "")
            ])
    print(f"FanDuel salary data saved to: {output_file}")

except Exception as e:
    print(f"Error fetching FanDuel salaries: {e}")
