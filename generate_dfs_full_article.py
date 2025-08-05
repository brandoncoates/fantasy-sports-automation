import json
import boto3
from datetime import datetime
from collections import defaultdict
import os


def load_json(filename):
    with open(filename, "r") as f:
        return json.load(f)


def get_weighted_trend_score(player):
    trend = player.get("trend_averages", {})
    last3 = trend.get("last_3", {}).get("Hits", 0)
    last6 = trend.get("last_6", {}).get("Hits", 0)
    last9 = trend.get("last_9", {}).get("Hits", 0)
    return (last3 * 0.5) + (last6 * 0.3) + (last9 * 0.2)


def get_streak_status(player):
    streak = player.get("streak_data", {})
    if streak.get("current_hot_streak", 0) >= 3:
        return "hot"
    elif streak.get("current_cold_streak", 0) >= 3:
        return "cold"
    return "neutral"


def build_note(player, role, score):
    streak = get_streak_status(player)
    trend = player.get("trend_averages", {})
    last3 = trend.get("last_3", {}).get("Hits", 0)
    last6 = trend.get("last_6", {}).get("Hits", 0)
    last9 = trend.get("last_9", {}).get("Hits", 0)
    opponent = player.get("opponent_team", "unknown opponent")
    note = f"3/6/9 game hit avg: {last3:.1f}/{last6:.1f}/{last9:.1f}. "

    if role == "fade":
        if streak == "cold":
            note += "Cold streak with minimal production. Fade candidate."
        else:
            note += f"Facing {opponent}, trend score is low. Risky play."
    else:
        if streak == "hot":
            note += f"Hot streak and trending up. Good matchup vs {opponent}."
        else:
            note += f"Decent recent trends with upside vs {opponent}."

    return note


def pick_players_by_position(players, position, num_targets=3, num_fades=1):
    filtered = [p for p in players if p.get("position") == position]
    sorted_players = sorted(filtered, key=get_weighted_trend_score, reverse=True)
    targets = sorted_players[:num_targets]
    fades = sorted_players[-num_fades:]
    return targets, fades


def upload_to_s3(local_path, bucket_name, s3_key):
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket_name, s3_key)
    print(f"✅ Uploaded to s3://{bucket_name}/{s3_key}")


def generate_full_dfs_article(enhanced_file, dfs_article_file, full_article_file, bucket_name):
    # Load input files
    enhanced_data = load_json(enhanced_file)
    dfs_article_data = load_json(dfs_article_file)
    full_article_data = load_json(full_article_file)

    date_str = dfs_article_data["date"]
    output_file = f"mlb_dfs_full_article_{date_str}.json"
    s3_key = f"baseball/full_mlb_articles/{output_file}"

    # Build player lookup
    player_pool = {
        name: data for name, data in enhanced_data.items()
        if data.get("roster_status", {}).get("status_code") == "A"
    }

    # Pitcher targets (only probable starters, one per team)
    probable_pitchers = [
        (name, get_weighted_trend_score(data))
        for name, data in player_pool.items()
        if data.get("position") == "P" and data.get("is_probable_starter")
    ]
    probable_pitchers = sorted(probable_pitchers, key=lambda x: x[1], reverse=True)

    used_teams = set()
    filtered_pitchers = []
    for name, _ in probable_pitchers:
        team = player_pool[name]["team"]
        if team not in used_teams:
            used_teams.add(team)
            filtered_pitchers.append(name)
        if len(filtered_pitchers) >= 5:
            break

    pitcher_fades = [
        name for name, _ in sorted(probable_pitchers, key=lambda x: x[1])[:2]
    ]

    # Infielders
    infield_positions = ["C", "1B", "2B", "3B", "SS"]
    infield_targets = defaultdict(list)
    infield_fades = {}

    for pos in infield_positions:
        targets, fades = pick_players_by_position(
            player_pool.values(), pos, num_targets=4, num_fades=1
        )
        infield_targets[pos] = targets
        infield_fades[pos] = fades

    # Outfielders
    outfield_positions = ["LF", "CF", "RF", "OF"]
    outfield_players = [
        p for p in player_pool.values() if p.get("position") in outfield_positions
    ]
    sorted_outfield = sorted(outfield_players, key=get_weighted_trend_score, reverse=True)
    outfield_targets = sorted_outfield[:5]
    outfield_fades = sorted_outfield[-2:]

    # Build output
    output = {
        "date": date_str,
        "pitchers": {"targets": [], "fades": []},
        "infielders": {"targets": {}, "fades": {}},
        "outfielders": {"targets": [], "fades": []}
    }

    for name in filtered_pitchers:
        player = player_pool[name]
        score = get_weighted_trend_score(player)
        output["pitchers"]["targets"].append({
            "name": name,
            "team": player["team"],
            "opponent": player.get("opponent_team", ""),
            "type": "Cash" if get_streak_status(player) != "cold" else "GPP",
            "notes": build_note(player, "target", score)
        })

    for name in pitcher_fades:
        player = player_pool[name]
        score = get_weighted_trend_score(player)
        output["pitchers"]["fades"].append({
            "name": name,
            "team": player["team"],
            "opponent": player.get("opponent_team", ""),
            "type": "GPP",
            "notes": build_note(player, "fade", score)
        })

    for pos in infield_positions:
        output["infielders"]["targets"][pos] = []
        for p in infield_targets[pos]:
            score = get_weighted_trend_score(p)
            output["infielders"]["targets"][pos].append({
                "name": p["name"],
                "team": p["team"],
                "opponent": p.get("opponent_team", ""),
                "type": "Cash" if get_streak_status(p) != "cold" else "GPP",
                "notes": build_note(p, "target", score)
            })

        output["infielders"]["fades"][pos] = []
        for p in infield_fades[pos]:
            score = get_weighted_trend_score(p)
            output["infielders"]["fades"][pos].append({
                "name": p["name"],
                "team": p["team"],
                "opponent": p.get("opponent_team", ""),
                "type": "GPP",
                "notes": build_note(p, "fade", score)
            })

    for p in outfield_targets:
        score = get_weighted_trend_score(p)
        output["outfielders"]["targets"].append({
            "name": p["name"],
            "team": p["team"],
            "opponent": p.get("opponent_team", ""),
            "type": "Cash" if get_streak_status(p) != "cold" else "GPP",
            "notes": build_note(p, "target", score)
        })

    for p in outfield_fades:
        score = get_weighted_trend_score(p)
        output["outfielders"]["fades"].append({
            "name": p["name"],
            "team": p["team"],
            "opponent": p.get("opponent_team", ""),
            "type": "GPP",
            "notes": build_note(p, "fade", score)
        })

    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    upload_to_s3(output_file, bucket_name, s3_key)


# Example run (you will set these paths correctly in your GitHub Actions)
# generate_full_dfs_article(
#     "enhanced_structured_players_2025-08-05.json",
#     "mlb_dfs_article_2025-08-05.json",
#     "mlb_dfs_full_article_2025-08-04.json",
#     bucket_name="your-s3-bucket-name"
# )
