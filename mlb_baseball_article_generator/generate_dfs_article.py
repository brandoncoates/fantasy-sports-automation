import json
import boto3
from datetime import datetime, timedelta, UTC
from collections import defaultdict
import os

# === Icons ===
ICON_TARGET = "✅"
ICON_FADE = "🚫"
ICON_NEUTRAL = "⚪"
ICON_HIT = "🔥"
ICON_MISS = "❌"

# ---------- Utilities ----------
def load_json(filename):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)

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

def evaluate_previous_article(article, previous_player_data):
    recap_summary = []

    def evaluate_players(section):
        for group in section:
            if isinstance(section[group], list):
                for player in section[group]:
                    name = player.get("name")
                    pdata = previous_player_data.get(name, {})
                    box = pdata.get("box_score", {})
                    result = score_player(box)
                    player["result"] = result
                    player["result_icon"] = ICON_HIT if result == "Hit" else ICON_MISS if result == "Miss" else ICON_NEUTRAL
                    player["box_score"] = box
                    recap_summary.append({"name": name, "team": player.get("team"), "position": player.get("position"), "result": result})
            elif isinstance(section[group], dict):
                for pos in section[group]:
                    for player in section[group][pos]:
                        name = player.get("name")
                        pdata = previous_player_data.get(name, {})
                        box = pdata.get("box_score", {})
                        result = score_player(box)
                        player["result"] = result
                        player["result_icon"] = ICON_HIT if result == "Hit" else ICON_MISS if result == "Miss" else ICON_NEUTRAL
                        player["box_score"] = box
                        recap_summary.append({"name": name, "team": player.get("team"), "position": pos, "result": result})

    evaluate_players(article)
    return article, recap_summary

def generate_full_dfs_article(enhanced_file, dfs_article_file, full_article_file, bucket_name):
    enhanced_data = load_json(enhanced_file)
    dfs_article_data = load_json(dfs_article_file)

    # Evaluate previous day's article
    yday = (datetime.strptime(dfs_article_data["date"], "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    yday_article_path = f"mlb_dfs_full_articles/mlb_dfs_full_article_{yday}.json"
    yday_enhanced_path = f"baseball/combined/enhanced_structured_players_{yday}.json"
    recap_summary = []
    if os.path.exists(yday_article_path) and os.path.exists(yday_enhanced_path):
        previous_player_data = load_json(yday_enhanced_path)
        with open(yday_article_path, "r") as f:
            yday_article = json.load(f)
        yday_article, recap_summary = evaluate_previous_article(yday_article, previous_player_data)
        with open(yday_article_path, "w") as f:
            json.dump(yday_article, f, indent=2)
        print(f"✅ Evaluated and saved results to {yday_article_path}")

    # Pick logic
    def get_trend_score(player):
        trend = player.get("trend_averages", {})
        return (
            1.0 * trend.get("last_3", 0.0)
            + 0.6 * trend.get("last_6", 0.0)
            + 0.3 * trend.get("last_9", 0.0)
        )

    def get_streak_label(player):
        sd = player.get("streak_data", {})
        if sd.get("current_hot_streak", 0) >= 3:
            return "hot"
        elif sd.get("current_cold_streak", 0) >= 3:
            return "cold"
        return "neutral"

    def build_note(player):
        trend = player.get("trend_averages", {})
        opponent = player.get("opponent_team", "?")
        streak = get_streak_label(player)
        note = f"3/6/9 game FD avg: {trend.get('last_3', 0.0):.1f}/{trend.get('last_6', 0.0):.1f}/{trend.get('last_9', 0.0):.1f}. "
        note += f"Streak: {streak}. Opponent: {opponent}."
        return note

    player_pool = {
        name: data for name, data in enhanced_data.items()
        if data.get("roster_status", {}).get("status_code") == "A"
    }

    probable_pitchers = [
        (name, get_trend_score(data))
        for name, data in player_pool.items()
        if data.get("position") == "P" and data.get("is_probable_starter")
    ]
    probable_pitchers = sorted(probable_pitchers, key=lambda x: x[1], reverse=True)

    used_teams = set()
    pitcher_targets = []
    for name, _ in probable_pitchers:
        team = player_pool[name]["team"]
        if team not in used_teams:
            used_teams.add(team)
            pitcher_targets.append(name)
        if len(pitcher_targets) >= 5:
            break

    pitcher_fades = [name for name, _ in sorted(probable_pitchers, key=lambda x: x[1])[:2]]

    output = {
        "date": dfs_article_data["date"],
        "recap_summary": recap_summary,
        "pitchers": {"targets": [], "fades": []}
    }

    for name in pitcher_targets:
        p = player_pool[name]
        output["pitchers"]["targets"].append({
            "name": name,
            "team": p.get("team"),
            "opponent": p.get("opponent_team"),
            "type": "Cash" if get_streak_label(p) != "cold" else "GPP",
            "notes": build_note(p)
        })

    for name in pitcher_fades:
        p = player_pool[name]
        output["pitchers"]["fades"].append({
            "name": name,
            "team": p.get("team"),
            "opponent": p.get("opponent_team"),
            "type": "GPP",
            "notes": build_note(p)
        })

    output_filename = f"mlb_dfs_full_article_{dfs_article_data['date']}.json"
    output_dir = "mlb_dfs_full_articles"
    os.makedirs(output_dir, exist_ok=True)
    local_path = os.path.join(output_dir, output_filename)
    s3_key = f"baseball/full_mlb_articles/{output_filename}"

    print(f"💾 Writing DFS full article to {local_path}")
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"✅ File written: {local_path}")

    print(f"☁️ Uploading to s3://{bucket_name}/{s3_key}")
    upload_to_s3(local_path, bucket_name, s3_key)

def generate_full_article(date_str):
    dfs_article_file = f"baseball/combined/mlb_dfs_article_{date_str}.json"
    enhanced_file = f"baseball/combined/enhanced_structured_players_{date_str}.json"
    full_article_file = f"baseball/combined/mlb_dfs_full_article_{date_str}.json"

    if not (os.path.exists(dfs_article_file) and os.path.exists(enhanced_file)):
        print(f"⚠️ One or more required files not found for {date_str}. Falling back to yesterday.")
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
    today_str = datetime.now(UTC).strftime("%Y-%m-%d")
    print(f"🚀 Running full article generation for {today_str}")
    generate_full_article(today_str)
