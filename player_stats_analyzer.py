import json
import os
from datetime import datetime
from collections import defaultdict

STRUCTURED_DIR = "baseball/combined"
ARCHIVE_DIR = "baseball/combined/archive"

def load_latest_structured_file():
    search_dirs = [STRUCTURED_DIR, "."]
    candidates = []
    for d in search_dirs:
        if not os.path.exists(d):
            continue
        files = [f for f in os.listdir(d) if f.startswith("structured_players_") and f.endswith(".json")]
        for f in files:
            candidates.append((f, os.path.join(d, f)))
    if not candidates:
        raise FileNotFoundError("❌ No structured player files found.")
    candidates.sort(reverse=True)
    latest_filename, latest_path = candidates[0]
    print(f"📄 Found latest structured file: {latest_filename}")
    with open(latest_path, "r", encoding="utf-8") as f:
        return json.load(f), latest_filename

def load_archive_stats():
    archive_stats = defaultdict(list)
    if not os.path.exists(ARCHIVE_DIR):
        print(f"⚠️ Archive directory {ARCHIVE_DIR} does not exist. Skipping.")
        return archive_stats
    for fname in os.listdir(ARCHIVE_DIR):
        if fname.startswith("structured_players_") and fname.endswith(".json"):
            with open(os.path.join(ARCHIVE_DIR, fname), "r", encoding="utf-8") as f:
                data = json.load(f)
                for player, stats in data.items():
                    if stats.get("box_score"):
                        archive_stats[player].append(stats)
    return archive_stats

def compute_recent_averages(historical):
    recent = historical[-3:]
    totals = defaultdict(float)
    counts = defaultdict(int)
    for entry in recent:
        for k, v in entry.get("box_score", {}).items():
            try:
                val = float(v)
                totals[k] += val
                counts[k] += 1
            except (ValueError, TypeError):
                continue
    return {k: round(totals[k] / counts[k], 2) for k in totals if counts[k] > 0}

def detect_streak_lengths(historical):
    streak_type = None
    streak_count = 0
    max_hot = max_cold = 0
    current = {"hot": 0, "cold": 0}

    for game in reversed(historical):
        box = game.get("box_score", {})
        fd = float(box.get("FanDuel Points", 0))
        if fd >= 25:
            if streak_type == "hot":
                streak_count += 1
            else:
                streak_type = "hot"
                streak_count = 1
            current["hot"] = streak_count
            max_hot = max(max_hot, streak_count)
        elif fd <= 8:
            if streak_type == "cold":
                streak_count += 1
            else:
                streak_type = "cold"
                streak_count = 1
            current["cold"] = streak_count
            max_cold = max(max_cold, streak_count)
        else:
            streak_type = None
            streak_count = 0

    return {
        "current_hot_streak": current["hot"],
        "current_cold_streak": current["cold"],
        "longest_hot_streak": max_hot,
        "longest_cold_streak": max_cold
    }

def detect_trend_labels(historical):
    labels = []
    recent = historical[-5:]
    hits = homers = rb_is = runs = xbh = games_with_hit = total_fd = fd_games = 0

    for game in recent:
        box = game.get("box_score", {})
        h = float(box.get("Hits", 0))
        hr = float(box.get("Home Runs", 0))
        rbi = float(box.get("Runs Batted In", 0))
        r = float(box.get("Runs", 0))
        dbl = float(box.get("Doubles", 0))
        tpl = float(box.get("Triples", 0))
        fd = float(box.get("FanDuel Points", 0))

        hits += h
        homers += hr
        rb_is += rbi
        runs += r
        xbh += (dbl + tpl + hr)

        if h > 0:
            games_with_hit += 1
        if fd > 0:
            total_fd += fd
            fd_games += 1

    avg_fd = total_fd / fd_games if fd_games else 0

    if games_with_hit >= 3:
        labels.append("hit_streak")
    if homers >= 3:
        labels.append("power_surge")
    if avg_fd >= 25:
        labels.append("hot_streak")
    if avg_fd < 8:
        labels.append("cold_streak")
    if rb_is == 0 and runs == 0 and xbh == 0:
        labels.append("slump")

    return labels

if __name__ == "__main__":
    print("🔍 Analyzing player trends and streaks...")
    archive = load_archive_stats()
    structured_today, filename = load_latest_structured_file()

    enhanced = {}
    for name, pdata in structured_today.items():
        historical = archive.get(name, [])
        pdata["recent_trends"] = compute_recent_averages(historical) if historical else {}
        pdata["trend_labels"] = detect_trend_labels(historical) if historical else []
        pdata["streak_data"] = detect_streak_lengths(historical) if historical else {}
        enhanced[name] = pdata

    out_path = os.path.join(STRUCTURED_DIR, f"enhanced_{filename}")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(enhanced, f, indent=2)

    print(f"✅ Saved enhanced player file with streak insights to {out_path}")
