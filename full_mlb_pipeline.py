# full_mlb_pipeline.py

import os
import json
from datetime import datetime

# Paths
ROSTERS_DIR = "rosters"
STARTERS_DIR = "probablestarters"
WEATHER_DIR = "weather"
BETTING_DIR = "betting"
NEWS_DIR = "news"
BOXSCORE_DIR = "boxscores"
OUTPUT_DIR = "baseball/combined"

# Util: Load latest file from a directory
def load_latest_json(directory):
    files = sorted(
        [f for f in os.listdir(directory) if f.endswith(".json")],
        reverse=True
    )
    if not files:
        print(f"⚠️ No JSON files found in {directory}")
        return {}
    with open(os.path.join(directory, files[0]), "r", encoding="utf-8") as f:
        return json.load(f)

TEAM_NAME_MAP = {
    "A's": "Athletics",
    "Athletics": "Athletics",
    "Braves": "Braves",
    "Orioles": "Orioles",
    "Red Sox": "Red Sox",
    "White Sox": "White Sox",
    "Cubs": "Cubs",
    "Reds": "Reds",
    "Guardians": "Guardians",
    "Indians": "Guardians",
    "Rockies": "Rockies",
    "Tigers": "Tigers",
    "Astros": "Astros",
    "Royals": "Royals",
    "Angels": "Angels",
    "Dodgers": "Dodgers",
    "Marlins": "Marlins",
    "Brewers": "Brewers",
    "Twins": "Twins",
    "Yankees": "Yankees",
    "Mets": "Mets",
    "Phillies": "Phillies",
    "Pirates": "Pirates",
    "Cardinals": "Cardinals",
    "Padres": "Padres",
    "Giants": "Giants",
    "Mariners": "Mariners",
    "Rays": "Rays",
    "Blue Jays": "Blue Jays",
    "Nationals": "Nationals",
    "D-backs": "Diamondbacks",
    "Diamondbacks": "Diamondbacks",
    "AZ": "Diamondbacks",
    "WSH": "Nationals",
    "NYM": "Mets",
    "NYY": "Yankees",
    "BOS": "Red Sox",
    "CHC": "Cubs",
    "CWS": "White Sox",
    "LAD": "Dodgers",
    "LAA": "Angels",
    "OAK": "Athletics",
    "SEA": "Mariners",
    "SF": "Giants",
    "SD": "Padres",
    "COL": "Rockies",
    "KC": "Royals",
    "MIL": "Brewers",
    "MIN": "Twins",
    "DET": "Tigers",
    "CLE": "Guardians",
    "HOU": "Astros",
    "ATL": "Braves",
    "PHI": "Phillies",
    "PIT": "Pirates",
    "STL": "Cardinals",
    "CIN": "Reds",
    "TB": "Rays",
    "TOR": "Blue Jays",
    "MIA": "Marlins"
}

def normalize_team(name):
    return TEAM_NAME_MAP.get(name, name)

# Main combine logic
def build_structured_players():
    print("🔧 Building structured player file...")

    rosters = load_latest_json(ROSTERS_DIR)
    starters = load_latest_json(STARTERS_DIR)
    weather = load_latest_json(WEATHER_DIR)
    betting = load_latest_json(BETTING_DIR)
    news = load_latest_json(NEWS_DIR)
    boxscores = load_latest_json(BOXSCORE_DIR)

    players = {}

    for player_id, pdata in rosters.items():
        team = normalize_team(pdata.get("team"))
        opponent = normalize_team(pdata.get("opponent_team"))

        player = {
            "player_id": player_id,
            "name": pdata.get("name"),
            "team": team,
            "opponent_team": opponent,
            "home_or_away": pdata.get("home_or_away"),
            "position": pdata.get("position"),
            "handedness": pdata.get("handedness"),
            "roster_status": pdata.get("status"),
            "is_probable_starter": pdata.get("is_probable_starter", False),
            "starter": pdata.get("starter", False),
            "weather_context": weather.get(team, {}),
            "betting_context": betting.get(team, {}),
            "espn_mentions": news.get(player_id, {}).get("mentions", 0),
            "espn_articles": news.get(player_id, {}).get("articles", []),
            "reddit_mentions": news.get(player_id, {}).get("reddit_mentions", 0),
            "box_score": boxscores.get(player_id, {})
        }
        players[player_id] = player

    print(f"✅ Combined {len(players)} players.")
    return players

# Output
def save_to_file(players):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    path = os.path.join(OUTPUT_DIR, f"structured_players_{date_str}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(players, f, indent=2)
    print(f"💾 Saved structured player file to: {path}")

    # 🧠 Generate and upload full DFS article
    from mlb_baseball_article_generator.generate_dfs_full_article import generate_full_article
    print("🧠 Generating full DFS article...")
    generate_full_article(date_str)
    print("✅ Full DFS article generated and uploaded.")

# Run script
if __name__ == "__main__":
    players = build_structured_players()
    save_to_file(players)

