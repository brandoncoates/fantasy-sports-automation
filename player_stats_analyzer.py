# player_stats_analyzer.py

import json
import os
from datetime import datetime
from collections import defaultdict

# Configuration
STRUCTURED_DIR = "baseball/combined"
ARCHIVE_DIR = "baseball/combined/archive"

# Load latest file by date
def load_latest_structured_file():
    files = [f for f in os.listdir(STRUCTURED_DIR) if f.startswith("structured_players_") and f.endswith(".json")]
    files.sort(reverse=True)
    if not files:
        raise FileNotFoundError("No structured player files found.")
    latest = files[0]
    full_path = os.path.join(STRUCTURED_DIR, latest)
    with open(full_path, "r", encoding="utf-8") as f:
        return json.load(f), latest

# Load archive stats
def load_archive_stats():
    archive_stats = defaultdict(list)
    for fname in os.listdir(ARCHIVE_DIR):
        if fname.startswith("structured_players_") and fname.endswith(".json"):
            with open(os.path.join(ARCHIVE_DIR, fname), "r", encoding="utf-8") as f:
                data = json.load(f)
                for player, stats in data.items():
                    if stats.get("box_score"):
                        archive_stats[player].append(stats)
    return archive_stats

# Compute recent average stats
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

    averages = {k: round(totals[k] / counts[k], 2) for k in totals if counts[k] > 0}
    return averages

# Detect trends and streaks
def detect_trends(historical):
    labels = []
    recent = historical[-5:]

    hits = 0
    homers = 0
    rb_is = 0
    runs = 0
    xbh = 0
    games_with_hit = 0
    total_fd = 0.0
    fd_games = 0

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

# Main script
if __name__ == "__main__":
    print("🔍 Analyzing player trends across archive...")

    archive = load_archive_stats()
    structured_today, filename = load_latest_structured_file()

    enhanced = {}
    for name, pdata in structured_today.items():
        historical = archive.get(name, [])
        trends = compute_recent_averages(historical) if historical else {}
        labels = detect_trends(historical) if historical else []
        pdata["recent_trends"] = trends
        pdata["trend_labels"] = labels
        enhanced[name] = pdata

    # Save to expected filename format
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    out_file = os.path.join(STRUCTURED_DIR, f"enhanced_structured_players_{today_str}.json")

    os.makedirs(STRUCTURED_DIR, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(enhanced, f, indent=2)

    print(f"✅ Saved enhanced player file to {out_file}")
