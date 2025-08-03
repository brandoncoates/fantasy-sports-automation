#!/usr/bin/env python3
import json
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
import os

# Constants
STRUCTURED_DIR = "baseball/combined"

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize(text: str) -> str:
    return (text or "").strip().lower().replace(" ", "").replace("-", "").replace(".", "").replace("'", "")

def extract_valid_matchups(players):
    matchups = set()
    for p in players.values():
        if p.get("position") == "P" and p.get("is_probable_starter") and p.get("team") and p.get("opponent_team"):
            teams = tuple(sorted([p["team"], p["opponent_team"]]))
            matchups.add(teams)
    return matchups

def filter_players_by_matchups(players, valid_matchups):
    valid_teams = {team for matchup in valid_matchups for team in matchup}
    return {
        k: v for k, v in players.items()
        if v.get("team") in valid_teams and v.get("opponent_team") in valid_teams
    }

def generate_notes(player):
    labels = player.get("trend_labels", [])
    notes = []

    if "hot_streak" in labels:
        notes.append("🔥 Riding a hot streak")
    if "cold_streak" in labels:
        notes.append("❄️ Struggling lately")
    if "hit_streak" in labels:
        notes.append("✅ Active hit streak")
    if "power_surge" in labels:
        notes.append("🧨 Hitting for power")
    if "slump" in labels:
        notes.append("🧊 Power numbers down")

    espn_mentions = player.get("espn_mentions", 0)
    reddit_mentions = player.get("reddit_mentions", 0)

    if espn_mentions > 0:
        notes.append(f"📣 Featured in {espn_mentions} ESPN article(s)")
    if reddit_mentions > 0:
        notes.append(f"💬 Discussed on Reddit")

    return "; ".join(notes)

def validate_pitcher_recommendations(recs):
    seen_teams = set()
    validated = []
    for rec in recs:
        if rec["position"] != "P":
            validated.append(rec)
            continue
        team = rec.get("team")
        if not team or team in seen_teams:
            continue
        seen_teams.add(team)
        validated.append(rec)
    return validated

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()

    date = args.date
    yday = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    # Point to correct directory
    structured_fp = os.path.join(STRUCTURED_DIR, f"enhanced_structured_players_{date}.json")
    recap_fp = os.path.join(STRUCTURED_DIR, f"mlb_dfs_article_{yday}.json")

    print(f"📂 Loading: {structured_fp}, {recap_fp}")

    if not os.path.exists(structured_fp):
        raise FileNotFoundError(f"Missing file: {structured_fp}")
    if not os.path.exists(recap_fp):
        raise FileNotFoundError(f"Missing file: {recap_fp}")

    players = load_json(structured_fp)
    recap = load_json(recap_fp)

    matchups = extract_valid_matchups(players)
    print(f"✅ Found {len(matchups)} valid matchups.")

    filtered_players = filter_players_by_matchups(players, matchups)
    print(f"✅ Filtered to {len(filtered_players)} players involved in valid matchups.")

    player_recommendations = []

    for name, player in filtered_players.items():
        if not player.get("team") or not player.get("opponent_team"):
            continue
        if player.get("position") == "P" and not player.get("is_probable_starter"):
            continue
        trends = player.get("weighted_trends") or player.get("recent_trends") or {}
        if not trends:
            continue

        trend_score = sum(trends.values())
        note = generate_notes(player)
        player_recommendations.append({
            "name": name,
            "team": player.get("team"),
            "opponent": player.get("opponent_team"),
            "position": player.get("position"),
            "trend_score": round(trend_score, 2),
            "notes": note
        })

    print(f"🎯 Total candidates before validation: {len(player_recommendations)}")

    validated_recs = validate_pitcher_recommendations(player_recommendations)
    print(f"✅ Final validated recommendations: {len(validated_recs)}")

    top_by_pos = defaultdict(list)
    for rec in sorted(validated_recs, key=lambda x: x["trend_score"], reverse=True):
        pos = rec["position"]
        if len(top_by_pos[pos]) < 3:
            top_by_pos[pos].append(rec)

    article = {
        "date": date,
        "matchups": sorted(list(matchups)),
        "num_valid_players": len(filtered_players),
        "recap_summary": recap.get("recap_summary", []),
        "recommendations": top_by_pos,
        "status": "assembled with validated logic"
    }

    out_fp = os.path.join(STRUCTURED_DIR, f"mlb_dfs_article_{date}.json")
    with open(out_fp, "w", encoding="utf-8") as f:
        json.dump(article, f, indent=2)
    print(f"✅ DFS article saved to {out_fp}")

if __name__ == "__main__":
    main()
