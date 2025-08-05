#!/usr/bin/env python3
import json
import os
from datetime import datetime
from collections import defaultdict

# ---- PATHS / NAMING (do not change without updating the workflow) ----
STRUCTURED_DIR = "baseball/combined"
ARCHIVE_DIR = "baseball/combined/archive"
STRUCTURED_PREFIX = "structured_players_"
ENHANCED_PREFIX = "enhanced_structured_players_"

# ---- UTILITIES ----
def safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def ensure_archive_copy(structured_path, structured_filename):
    """Make sure today's structured file is also in archive/ for trend history."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    archive_target = os.path.join(ARCHIVE_DIR, structured_filename)
    if not os.path.exists(archive_target):
        try:
            # Hard link if available, else copy bytes
            os.link(structured_path, archive_target)
        except OSError:
            import shutil
            shutil.copy2(structured_path, archive_target)
        print(f"📦 Archived {structured_filename} -> {archive_target}")
    else:
        print(f"📦 Archive already has {structured_filename}")

def load_latest_structured_file():
    """
    Finds latest structured_players_YYYY-MM-DD.json in STRUCTURED_DIR.
    Returns (data, filename, fullpath).
    """
    if not os.path.exists(STRUCTURED_DIR):
        raise FileNotFoundError(f"❌ Missing directory: {STRUCTURED_DIR}")

    candidates = []
    for f in os.listdir(STRUCTURED_DIR):
        if f.startswith(STRUCTURED_PREFIX) and f.endswith(".json"):
            candidates.append(f)
    if not candidates:
        raise FileNotFoundError("❌ No structured player files found.")

    # Sort lexicographically (YYYY-MM-DD sorts correctly)
    candidates.sort(reverse=True)
    latest_filename = candidates[0]
    latest_path = os.path.join(STRUCTURED_DIR, latest_filename)
    print(f"📄 Found latest structured file: {latest_filename}")

    with open(latest_path, "r", encoding="utf-8") as f:
        return json.load(f), latest_filename, latest_path

def load_archive_stats():
    """
    Loads all archived structured files and builds a per-player list (chronological)
    of historical entries. Only entries that include a box_score are used.
    """
    archive_stats = defaultdict(list)
    if not os.path.exists(ARCHIVE_DIR):
        print(f"⚠️ Archive directory {ARCHIVE_DIR} does not exist. Skipping.")
        return archive_stats

    # Sort ascending so history is chronological
    for fname in sorted(os.listdir(ARCHIVE_DIR)):
        if fname.startswith(STRUCTURED_PREFIX) and fname.endswith(".json"):
            with open(os.path.join(ARCHIVE_DIR, fname), "r", encoding="utf-8") as f:
                data = json.load(f)
                for player, stats in data.items():
                    if stats.get("box_score"):
                        archive_stats[player].append(stats)
    return archive_stats

def compute_recent_averages(historical, n):
    """
    Compute averages from the last n games (by archive order).
    Falls back gracefully if fewer than n games exist.
    """
    if not historical:
        return {}
    recent = historical[-n:]
    totals = defaultdict(float)
    counts = defaultdict(int)
    for entry in recent:
        for k, v in entry.get("box_score", {}).items():
            val = safe_float(v, None)
            if val is None:
                continue
            totals[k] += val
            counts[k] += 1
    return {k: round(totals[k] / counts[k], 2) for k in totals if counts[k] > 0}

def detect_streak_lengths_and_ranges(historical, hot_cut=25.0, cold_cut=8.0):
    """
    Walks games chronologically and detects:
    - current hot/cold streak lengths
    - longest hot/cold streaks
    - ranges of hot/cold streaks (each range holds length and a list of FD points)
    """
    if not historical:
        return {
            "current_hot_streak": 0,
            "current_cold_streak": 0,
            "longest_hot_streak": 0,
            "longest_cold_streak": 0,
            "hot_streaks": [],
            "cold_streaks": []
        }

    streak_type = None  # 'hot' or 'cold' or None
    streak_len = 0
    max_hot = max_cold = 0
    current_hot = current_cold = 0
    hot_ranges, cold_ranges = [], []
    fd_accum = []  # FanDuel point values in the current streak

    # iterate in chronological order
    for game in historical:
        box = game.get("box_score", {})
        fd = safe_float(box.get("FanDuel Points", 0.0), 0.0)

        if fd >= hot_cut:
            # continuing/starting a hot streak
            if streak_type == "hot":
                streak_len += 1
            else:
                # close a cold streak if ongoing
                if streak_type == "cold" and fd_accum:
                    cold_ranges.append({"length": streak_len, "fd_points": fd_accum[:]})
                streak_type = "hot"
                streak_len = 1
                fd_accum = []
            fd_accum.append(fd)
        elif fd <= cold_cut:
            # continuing/starting a cold streak
            if streak_type == "cold":
                streak_len += 1
            else:
                # close a hot streak if ongoing
                if streak_type == "hot" and fd_accum:
                    hot_ranges.append({"length": streak_len, "fd_points": fd_accum[:]})
                streak_type = "cold"
                streak_len = 1
                fd_accum = []
            fd_accum.append(fd)
        else:
            # neutral result ends any current streak
            if streak_type == "hot" and fd_accum:
                hot_ranges.append({"length": streak_len, "fd_points": fd_accum[:]})
            elif streak_type == "cold" and fd_accum:
                cold_ranges.append({"length": streak_len, "fd_points": fd_accum[:]})
            streak_type = None
            streak_len = 0
            fd_accum = []

        # Track longest
        if streak_type == "hot":
            max_hot = max(max_hot, streak_len)
        elif streak_type == "cold":
            max_cold = max(max_cold, streak_len)

    # Close final streak if still open
    if streak_type == "hot" and fd_accum:
        hot_ranges.append({"length": streak_len, "fd_points": fd_accum[:]})
        current_hot = streak_len
        current_cold = 0
    elif streak_type == "cold" and fd_accum:
        cold_ranges.append({"length": streak_len, "fd_points": fd_accum[:]})
        current_cold = streak_len
        current_hot = 0
    else:
        current_hot = current_cold = 0

    return {
        "current_hot_streak": current_hot,
        "current_cold_streak": current_cold,
        "longest_hot_streak": max_hot,
        "longest_cold_streak": max_cold,
        "hot_streaks": hot_ranges,    # list of {length, fd_points: [..]}
        "cold_streaks": cold_ranges   # list of {length, fd_points: [..]}
    }

def detect_trend_labels(historical):
    """
    Label recent trends with guardrails for thin data.
    Uses last 5 games window if available; requires >= 3 games to apply labels.
    """
    if not historical or len(historical) < 3:
        return ["insufficient_data"]

    window = historical[-5:]
    games_with_hit = homers = runs = rbi = xbh = 0
    total_fd = 0.0
    fd_games = 0

    for g in window:
        box = g.get("box_score", {})
        h = safe_float(box.get("Hits", 0), 0.0)
        hr = safe_float(box.get("Home Runs", 0), 0.0)
        rr = safe_float(box.get("Runs", 0), 0.0)
        rb = safe_float(box.get("Runs Batted In", 0), 0.0)
        dbl = safe_float(box.get("Doubles", 0), 0.0)
        tpl = safe_float(box.get("Triples", 0), 0.0)
        fd = safe_float(box.get("FanDuel Points", 0), 0.0)

        if h > 0:
            games_with_hit += 1
        homers += hr
        runs += rr
        rbi += rb
        xbh += (dbl + tpl + hr)
        if fd > 0:
            total_fd += fd
            fd_games += 1

    labels = []
    avg_fd = (total_fd / fd_games) if fd_games > 0 else 0.0

    # Guardrails: only tag hot/cold if we have at least 3 FD-scored games
    if fd_games >= 3 and avg_fd >= 25:
        labels.append("hot_streak")
    if fd_games >= 3 and avg_fd < 8:
        labels.append("cold_streak")

    if games_with_hit >= 3:
        labels.append("hit_streak")
    if homers >= 3:
        labels.append("power_surge")
    if (rbi == 0 and runs == 0 and xbh == 0) and len(window) >= 3:
        labels.append("slump")

    return labels if labels else ["neutral_recent_form"]

# ---- MAIN ----
if __name__ == "__main__":
    print("🔍 Analyzing player trends and streaks...")

    # Load latest structured file from combined/
    structured_today, structured_filename, structured_path = load_latest_structured_file()

    # Ensure we have an archive copy for history computation
    ensure_archive_copy(structured_path, structured_filename)

    # Load archive history after ensuring today's file is archived (so today is countable)
    archive = load_archive_stats()

    enhanced = {}
    for name, pdata in structured_today.items():
        historical = archive.get(name, [])

        # 3/6/9 layered averages
        trend_averages = {
            "last_3": compute_recent_averages(historical, 3) if historical else {},
            "last_6": compute_recent_averages(historical, 6) if historical else {},
            "last_9": compute_recent_averages(historical, 9) if historical else {},
        }

        # Backward-compat: keep a 3-game "recent_trends"
        recent_trends_3 = trend_averages["last_3"]

        # Streaks (with ranges)
        streak_data = detect_streak_lengths_and_ranges(historical)

        # Labels (guarded for thin data)
        trend_labels = detect_trend_labels(historical)

        # Attach
        pdata["trend_averages"] = trend_averages
        pdata["recent_trends"] = recent_trends_3
        pdata["streak_data"] = streak_data
        pdata["trend_labels"] = trend_labels

        enhanced[name] = pdata

    # Output: enhanced_structured_players_YYYY-MM-DD.json
    # Use the same date from the structured filename.
    # structured_filename looks like 'structured_players_YYYY-MM-DD.json'
    date_str = structured_filename[len(STRUCTURED_PREFIX):-5]  # strip prefix & '.json'
    out_filename = f"{ENHANCED_PREFIX}{date_str}.json"
    out_path = os.path.join(STRUCTURED_DIR, out_filename)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(enhanced, f, indent=2)

    print(f"✅ Saved enhanced player file with streak insights to {out_path}")
