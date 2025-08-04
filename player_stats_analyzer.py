# player_stats_analyzer.py

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
        raise FileNotFoundError("❌ No structured player files found in combined/ or root directory.")

    candidates.sort(reverse=True)
    latest_filename, latest_path = candidates[0]
    print(f"📄 Found latest structured file: {latest_filename} at {latest_path}")
    with open(latest_path, "r", encoding="utf-8") as f:
        return json.load(f), latest_filename

def load_archive_stats():
    archive_stats = defaultdict(list)
    if not os.path.exists(ARCHIVE_DIR):
        print(f"⚠️ Archive directory {ARCHIVE_DIR} does not exist. Skipping archive load.")
        return archive_stats

    for fname in sorted(os.listdir(ARCHIVE_DIR)):
        if fname.startswith("structured_players_") and fname.endswith(".json"):
            with open(os.path.join(ARCHIVE_DIR, fname), "r", encoding="utf-8") as f:
                data = json.load(f)
                for player, stats in data.items():
                    if stats.get("box_score"):
                        archive_stats[player].append(stats)
    return archive_stats

def compute_recent_averages(historical, num_games=3):
    recent = historical[-num_games:]
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

def streak_lengths(historical, kind):
    streaks = []
    current = 0

    for game in historical:
        fd = float(game.get("box_score", {}).get("FanDuel Points", 0))
        if kind == "hot" and fd >= 25:
            current += 1
        elif kind == "cold" and fd < 8:
            current += 1
        else:
            if current:
                streaks.append(current)
            current = 0

    if current:
        streaks.append(current)
    return streaks

def detect_trends_and_streaks(historical):
    labels = []
    recent = historical[-5:]
    hits, homers, rb_is, runs, xbh = 0, 0, 0, 0, 0
    games_with_hit, total_fd, fd_games = 0, 0.0, 0

    current_streak_type = None
    current_streak_length = 0

    if historical:
        latest_fd = float(historical[-1].get("box_score", {}).get("FanDuel Points", 0))
        if latest_fd >= 25:
            current_streak_type = "hot"
        elif latest_fd < 8:
            current_streak_type = "cold"

        for game in reversed(historical):
            fd = float(game.get("box_score", {}).get("FanDuel Points", 0))
            if current_streak_type == "hot" and fd >= 25:
                current_streak_length += 1
            elif current_streak_type == "cold" and fd < 8:
                current_streak_length += 1
            else:
                break

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

    return {
        "trend_labels": labels,
        "current_streak_type": current_streak_type,
        "current_streak_length": current_streak_length,
        "hot_streak_lengths": streak_lengths(historical, "hot"),
        "cold_streak_lengths": streak_lengths(historical, "cold")
    }

if __name__ == "__main__":
    print("🔍 Analyzing player trends across archive...")
    archive = load_archive_stats()
    structured_today, filename = load_latest_structured_file()

    enhanced = {}
    for name, pdata in structured_today.items():
        historical = archive.get(name, [])
        trends = compute_recent_averages(historical) if historical else {}
        streak_info = detect_trends_and_streaks(historical) if historical else {}

        pdata["recent_trends"] = trends
        pdata["trend_labels"] = streak_info.get("trend_labels", [])
        pdata["current_streak_type"] = streak_info.get("current_streak_type")
        pdata["current_streak_length"] = streak_info.get("current_streak_length")
        pdata["hot_streak_lengths"] = streak_info.get("hot_streak_lengths", [])
        pdata["cold_streak_lengths"] = streak_info.get("cold_streak_lengths", [])

        enhanced[name] = pdata

    enhanced_filename = f"enhanced_{filename}"
    enhanced_path = os.path.join(STRUCTURED_DIR, enhanced_filename)
    with open(enhanced_path, "w", encoding="utf-8") as f:
        json.dump(enhanced, f, indent=2)

    print(f"✅ Saved enhanced player file with trends to {enhanced_path}")
