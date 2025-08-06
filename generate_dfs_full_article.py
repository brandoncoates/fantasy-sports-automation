import json
import boto3
from datetime import datetime, timedelta
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
    return sorted_players[:num_targets], sorted_players[-num_fades:]


def upload_to_s3(local_path, bucket_name, s3_key):
    if not os.path.exists(local_path):
        print(f"❌ File not found: {local_path}")
        return

    try:
        s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))
        s3.upload_file(local_path, bucket_name, s3_key)
        print(f"✅ Uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")


def generate_full_dfs_article(enhanced_file, dfs_article_file, full_article_file, bucket_name):
    enhanced_data = load_json(enhanced_file)
    dfs_article_data = load_json(dfs_article_file)
    full_article_data = load_json(full_article_file)

    date_str = dfs_article_data["date"]
    output_filename = f"mlb_dfs_full_article_{date_str}.json"
    output_dir = "mlb_dfs_full_articles"
    os.makedirs(output_dir, exist_ok=True)
    local_path = os.path.join(output_dir, output_filename)
    s3_key = f"baseball/full_mlb_articles/{output_filename}"

    player_pool = {
        name: data for name, data in enhanced_data.items()
        if data.get("roster_status", {}).get("status_code") == "A"
    }

    # Pitchers
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

    pitcher_fades = [name for name, _ in sorted(probable_pitchers, key=lambda x: x[1])[:2]]

    # Infield
    infield_positions = ["C", "1B", "2B", "3B", "SS"]
    infield_targets = defaultdict(list)
    infield_fades = {}
    for pos in infield_positions:
        targets, fades = pick_players_by_position(player_pool.values(), pos, 4, 1)
        infield_targets[pos] = targets
        infield_fades[pos] = fades

    # Outfield
    outfield_positions = ["LF", "CF", "RF", "OF"]
    outfield_players = [p for p in player_pool.values() if p.get("position") in outfield_positions]
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
        p = player_pool[name]
        output["pitchers"]["targets"].append({
            "name": name,
            "team": p["team"],
            "opponent": p.get("opponent_team", ""),
            "type": "Cash" if get_streak_status(p) != "cold" else "GPP",
            "notes": build_note(p, "target", get_weighted_trend_score(p))
        })

    for name in pitcher_fades:
        p = player_pool[name]
        output["pitchers"]["fades"].append({
            "name": name,
            "team": p["team"],
            "opponent": p.get("opponent_team", ""),
            "type": "GPP",
            "notes": build_note(p, "fade", get_weighted_trend_score(p))
        })

    for pos in infield_positions:
        output["infielders"]["targets"][pos] = []
        for p in infield_targets[pos]:
            output["infielders"]["targets"][pos].append({
                "name": p["name"],
                "team": p["team"],
                "opponent": p.get("opponent_team", ""),
                "type": "Cash" if get_streak_status(p) != "cold" else "GPP",
                "notes": build_note(p, "target", get_weighted_trend_score(p))
            })
        output["infielders"]["fades"][pos] = []
        for p in infield_fades[pos]:
            output["infielders"]["fades"][pos].append({
                "name": p["name"],
                "team": p["team"],
                "opponent": p.get("opponent_team", ""),
                "type": "GPP",
                "notes": build_note(p, "fade", get_weighted_trend_score(p))
            })

    for p in outfield_targets:
        output["outfielders"]["targets"].append({
            "name": p["name"],
            "team": p["team"],
            "opponent": p.get("opponent_team", ""),
            "type": "Cash" if get_streak_status(p) != "cold" else "GPP",
            "notes": build_note(p, "target", get_weighted_trend_score(p))
        })

    for p in outfield_fades:
        output["outfielders"]["fades"].append({
            "name": p["name"],
            "team": p["team"],
            "opponent": p.get("opponent_team", ""),
            "type": "GPP",
            "notes": build_note(p, "fade", get_weighted_trend_score(p))
        })

    print(f"💾 Writing DFS full article to {local_path}")
    with open(local_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"✅ File written: {local_path}")

    print(f"☁️ Uploading to s3://{bucket_name}/{s3_key}")
    upload_to_s3(local_path, bucket_name, s3_key)


def generate_full_article(date_str):
    # Start with today
    dfs_article_file = f"baseball/combined/mlb_dfs_article_{date_str}.json"
    enhanced_file = f"baseball/combined/enhanced_structured_players_{date_str}.json"

    if not os.path.exists(dfs_article_file):
        print(f"⚠️ DFS article file not found for {date_str}. Falling back to yesterday.")
        date_str = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        dfs_article_file = f"baseball/combined/mlb_dfs_article_{date_str}.json"
        enhanced_file = f"baseball/combined/enhanced_structured_players_{date_str}.json"

    full_article_file = f"baseball/combined/mlb_dfs_full_article_{date_str}.json"
    if not os.path.exists(full_article_file):
        print(f"⚠️ No full article file found for {date_str}, proceeding without previous data.")
        full_article_file = dfs_article_file

    bucket_name = "fantasy-sports-csvs"

    generate_full_dfs_article(
        enhanced_file=enhanced_file,
        dfs_article_file=dfs_article_file,
        full_article_file=full_article_file,
        bucket_name=bucket_name
    )


if __name__ == "__main__":
    from datetime import UTC
    today_str = datetime.now(UTC).strftime("%Y-%m-%d")
    print(f"🚀 Running full article generation for {today_str}")
    generate_full_article(today_str)


if __name__ == "__main__":
    from datetime import UTC
    today_str = datetime.now(UTC).strftime("%Y-%m-%d")
    print(f"🚀 Running full article generation for {today_str}")
    generate_full_article(today_str)
