import requests
import pandas as pd
from datetime import datetime
import boto3
import os

def fetch_action_props():
    url = "https://api.actionnetwork.com/web/v1/props/mlb"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch props: {response.status_code}")
        return pd.DataFrame()

    data = response.json().get("props", [])
    records = []

    for prop in data:
        try:
            player = prop.get("athlete", {}).get("display_name")
            team = prop.get("team", {}).get("display_abbr")
            opponent = prop.get("opponent_team", {}).get("display_abbr")
            market = prop.get("market", {}).get("name")
            prop_type = prop.get("label")
            line = prop.get("value")
            over_odds = prop.get("over", {}).get("american_odds")
            under_odds = prop.get("under", {}).get("american_odds")

            if market and prop_type and player:
                records.append({
                    "Player": player,
                    "Team": team,
                    "Opponent": opponent,
                    "Prop Type": prop_type,
                    "Line": line,
                    "Over Odds": over_odds,
                    "Under Odds": under_odds
                })
        except Exception as e:
            print("Error parsing prop:", e)

    return pd.DataFrame(records)

def save_to_s3(df):
    today_str = datetime.today().strftime('%Y-%m-%d')
    filename = f"action_network_props_{today_str}.csv"
    df.to_csv(filename, index=False)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    s3.upload_file(
        filename,
        "goatland-csvs"
        f"baseball/props-action/action_network_props_{today_str}.csv"
    )
    print(f"✅ Uploaded to S3: baseball/props-action/action_network_props_{today_str}.csv")

def main():
    print("🔍 Fetching Action Network props...")
    df = fetch_action_props()
    print(f"🧮 Total props scraped: {len(df)}")
    if not df.empty:
        save_to_s3(df)

if __name__ == "__main__":
    main()
