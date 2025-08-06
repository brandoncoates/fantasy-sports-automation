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

def evaluate_previous_article(prev_article_file, prev_enhanced_file):
    if not os.path.exists(prev_article_file) or not os.path.exists(prev_enhanced_file):
        print("⚠️ Previous article or enhanced file not found, skipping evaluation.")
        return []

    article = load_json(prev_article_file)
    player_data = load_json(prev_enhanced_file)
    recap = []

    def eval_section(section):
        for pos in section:
            for player in section[pos]:
                name = player.get("name")
                pdata = player_data.get(name, {})
                box = pdata.get("box_score", {})
                result = score_player(box)
                icon = ICON_HIT if result == "Hit" else ICON_MISS if result == "Miss" else ICON_NEUTRAL
                recap.append({
                    "player": name,
                    "team": player.get("team"),
                    "position": pos,
                    "result": f"{icon} {result}"
                })
    eval_section(article.get("recommendations", {}))
    return recap

def summarize_trends(enhanced_data):
    streaks = defaultdict(list)
    for name, p in enhanced_data.items():
        pos = p.get("position")
        if pos is None:
            continue
        if pos == "P" and not p.get("is_probable_starter"):
            continue

        labels = p.get("trend_labels", []) or []
        avgs = p.get("trend_averages", {})
        score = sum([float(v) for v in avgs.values() if isinstance(v, (int, float, str)) and str(v).replace('.','',1).isdigit()])

        tag = "neutral"
        if "cold_streak" in labels and avgs.get("last3", 0) > avgs.get("last9", 0):
            tag = "target"
        elif "hot_streak" in labels and avgs.get("last3", 0) < avgs.get("last9", 0):
            tag = "fade"

        streaks[pos].append({
            "name": name,
            "team": p.get("team"),
            "opponent": p.get("opponent_team"),
            "position": pos,
            "trend_score": round(score, 2),
            "tag": tag,
            "icon": ICON_TARGET if tag == "target" else ICON_FADE if tag == "fade" else ICON_NEUTRAL,
            "notes": "; ".join(labels),
            "trend_averages": avgs,
        })

    grouped = defaultdict(list)
    seen_sp_teams = set()
    for pos, items in streaks.items():
        for p in sorted(items, key=lambda x: x["trend_score"], reverse=True):
            if p["position"] == "P":
                team = p.get("team")
                if team in seen_sp_teams:
                    continue
                seen_sp_teams.add(team)
            grouped[pos].append(p)
    return grouped

def build_article(date_str, enhanced_file, dfs_article_file, output_file):
    enhanced_data = load_json(enhanced_file)
    article_data = load_json(dfs_article_file)
    yesterday = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_article_file = f"baseball/full_mlb_articles/mlb_dfs_full_article_{yesterday}.json"
    prev_enhanced_file = f"baseball/combined/enhanced_structured_players_{yesterday}.json"

    recap_summary = evaluate_previous_article(prev_article_file, prev_enhanced_file)
    recommendations = summarize_trends(enhanced_data)

    article_data["recap_summary"] = recap_summary
    article_data["recommendations"] = recommendations
    article_data["status"] = "Full article generated with evaluation and trend analysis."

    with open(output_file, "w") as f:
        json.dump(article_data, f, indent=2)
    print(f"✅ DFS full article written to {output_file}")

def generate_full_article(date_str):
    dfs_article_file = f"baseball/combined/mlb_dfs_article_{date_str}.json"
    enhanced_file = f"baseball/combined/enhanced_structured_players_{date_str}.json"
    full_article_file = f"mlb_dfs_full_articles/mlb_dfs_full_article_{date_str}.json"
    bucket_name = "fantasy-sports-csvs"
    s3_key = f"baseball/full_mlb_articles/mlb_dfs_full_article_{date_str}.json"

    if not os.path.exists(dfs_article_file) or not os.path.exists(enhanced_file):
        raise FileNotFoundError("Required files missing for today's article generation.")

    os.makedirs("mlb_dfs_full_articles", exist_ok=True)
    build_article(date_str, enhanced_file, dfs_article_file, full_article_file)
    upload_to_s3(full_article_file, bucket_name, s3_key)

if __name__ == "__main__":
    today_str = datetime.now(UTC).strftime("%Y-%m-%d")
    print(f"🚀 Running full article generation for {today_str}")
    generate_full_article(today_str)