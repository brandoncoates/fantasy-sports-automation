import pandas as pd
import datetime
import os

# Get today's date
today = datetime.date.today().strftime('%Y-%m-%d')

# DraftKings CSV URL for MLB contests
url = "https://www.draftkings.com/lineup/getavailableplayerscsv?contestType=MLB"

# Read the CSV into a DataFrame
try:
    df = pd.read_csv(url)
except Exception as e:
    print("Error downloading DraftKings salaries:", e)
    exit(1)

# Optional: Keep only the most relevant columns
columns_to_keep = [
    "Name", "Position", "TeamAbbrev", "AvgPointsPerGame", "Salary", "GameInfo", "InjuryIndicator"
]
df = df[columns_to_keep]

# Save path
output_dir = "Fantasy Baseball/Player Salaries"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, f"mlb_draftkings_salaries_{today}.csv")

# Save to CSV
df.to_csv(output_file, index=False)
print(f"DraftKings salary data saved to {output_file}")
