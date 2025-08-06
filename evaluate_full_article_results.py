import json
import os
from datetime import datetime

# === CONFIGURATION ===
today_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
article_path = f"mlb_dfs_full_articles/mlb_dfs_full_article_{today_str}.json"
player_path = f"baseball/combined/enhanced_structured_players_{today_str}.json"
out_path = f"mlb_dfs_full_articles/mlb_dfs_full_article_{today_str}.json"  # Overwrite

# === LOAD FILES ===
with open(article_path, "r") as f:
    article = json.load(f)

with open(player_path, "r") as f:
    players = json.load(f)

# === SCORING FUNCTION ===
def score_player(box):
    if not box:
        return "DNP"

    ab = box.get("AB", 0)
    h = box.get("H", 0)
    hr = box.get("HR", 0)
    rbi = box.get("RBI", 0)
    bb = box.get("BB", 0)

    if ab == 0 and h == 0:
        return "DNP"
    if h >= 2 or hr >= 1 or rbi >= 3:
        return "Hit"
    if h == 1 or bb >= 2:
        return "Neutral"
    return "Miss"

# === EVALUATE SECTION ===
def evaluate_players(section):
    for group in section:
        if isinstance(section[group], list):
            for player in section[group]:
                name = player["name"]
                pdata = players.get(name, {})
                box = pdata.get("box_score", {})
                player["result"] = score_player(box)
                player["box_score"] = box
        elif isinstance(section[group], dict):
            for pos in section[group]:
                for player in section[group][pos]:
                    name = player["name"]
                    pdata = players.get(name, {})
                    box = pdata.get("box_score", {})
                    player["result"] = score_player(box)
                    player["box_score"] = box

# === APPLY TO ALL SECTIONS ===
evaluate_players(article)

# === SAVE OUTPUT ===
with open(out_path, "w") as f:
    json.dump(article, f, indent=2)

print(f"✅ Evaluation complete. Results saved to {out_path}")
