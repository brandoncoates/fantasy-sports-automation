#!/usr/bin/env python3
import json
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
import os

# === Paths & Conventions ===
STRUCTURED_DIR = "baseball/combined"
DEFAULT_RECAP_DIR = "baseball/combined"  # where yesterday's FULL article lives

# === Icons ===
ICON_TARGET = "✅"
ICON_FADE = "🚫"
ICON_NEUTRAL = "⚪"
ICON_HIT = "🔥"
ICON_MISS = "❌"

# ---------- Utilities ----------
def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def get_fd_avgs(player):
    """
    Pull last 3/6/9 FanDuel average if present from analyzer output.
    Supports multiple possible keys so we don't break if upstream evolves.
    """
    avgs = player.get("trend_averages") or player.get("trend_avgs") or {}
    return {
        "last3": safe_float(avgs.get("last_3") or avgs.get("last3")),
        "last6": safe_float(avgs.get("last_6") or avgs.get("last6")),
        "last9": safe_float(avgs.get("last_9") or avgs.get("last9")),
    }

def get_base_trend_score(player):
    """
    Combine recent/weighted trends into one score.
    Fallback order: weighted_trends -> recent_trends -> 0
    """
    trends = player.get("weighted_trends") or player.get("recent_trends") or {}
    # Sum numeric values only
    score = 0.0
    for v in trends.values():
        try:
            score += float(v)
        except Exception:
            continue
    return score

def matchup_gate(players):
    """
    Build valid matchups from probable starters and restrict player pool to teams playing today.
    """
    matchups = set()
    for p in players.values():
        if p.get("position") == "P" and p.get("is_probable_starter") and p.get("team") and p.get("opponent_team"):
            teams = tuple(sorted([p["team"], p["opponent_team"]]))
            matchups.add(teams)

    valid_teams = {t for m in matchups for t in m}
    filtered = {
        k: v for k, v in players.items()
        if v.get("team") in valid_teams and v.get("opponent_team") in valid_teams
    }
    return matchups, filtered

def infer_tag(player):
    """
    Heuristic to assign Target/Fade/Neutral based on streak labels & momentum:

    - If on a cold_streak but last3 > last9 by a threshold -> TARGET (breakout signal)
    - If on a hot_streak but last3 < last9 by a threshold -> FADE (cooling signal)
    - Else NEUTRAL

    Thresholds can be tuned; keep modest to avoid overfitting on small samples.
    """
    labels = player.get("trend_labels", []) or []
    fd = get_fd_avgs(player)
    d3_vs_9 = fd["last3"] - (fd["last9"] if fd["last9"] else fd["last6"] or 0.0)

    # Small n guard: if no avgs, stay neutral
    has_any_avg = any([fd["last3"], fd["last6"], fd["last9"]])
    if not has_any_avg:
        return {"tag": "neutral", "icon": ICON_NEUTRAL, "reason": "insufficient data"}

    # Thresholds (points of FanDuel)
    IMPROVE_THRESH = 3.0   # trending up meaningfully
    DECLINE_THRESH = -3.0  # trending down meaningfully

    if "cold_streak" in labels and d3_vs_9 >= IMPROVE_THRESH:
        return {"tag": "target", "icon": ICON_TARGET, "reason": "cold streak but improving (last3 > last9)"}

    if "hot_streak" in labels and d3_vs_9 <= DECLINE_THRESH:
        return {"tag": "fade", "icon": ICON_FADE, "reason": "hot streak but cooling (last3 < last9)"}

    # Otherwise neutral
    return {"tag": "neutral", "icon": ICON_NEUTRAL, "reason": "no strong signal"}

def generate_notes(player):
    """
    Compose concise notes from labels and mentions.
    """
    labels = player.get("trend_labels", []) or []
    notes = []

    # Label snippets
    if "hot_streak" in labels:
        notes.append("Hot streak")
    if "cold_streak" in labels:
        notes.append("Cold streak")
    if "hit_streak" in labels:
        notes.append("Active hit streak")
    if "power_surge" in labels:
        notes.append("Power surge")
    if "slump" in labels:
        notes.append("Run production down")
    if "insufficient_data" in labels:
        notes.append("Limited data")

    # Mentions
    espn_mentions = int(player.get("espn_mentions") or 0)
    reddit_mentions = int(player.get("reddit_mentions") or 0)
    if espn_mentions > 0:
        notes.append(f"{espn_mentions} ESPN mention(s)")
    if reddit_mentions > 0:
        notes.append("Discussed on Reddit")

    # Streak details if present
    sd = player.get("streak_data") or {}
    ch = sd.get("current_hot_streak", 0)
    cc = sd.get("current_cold_streak", 0)
    if ch:
        notes.append(f"Current hot streak: {ch}")
    if cc:
        notes.append(f"Current cold streak: {cc}")

    return "; ".join(notes)

def summarize_recap(recap_full):
    """
    Convert yesterday's full article recap into compact icon+text entries if present.
    We expect recap to be either a list of dicts or a structure under keys like 'recap'/'recap_summary'.
    """
    if isinstance(recap_full, dict):
        if "recap_summary" in recap_full:
            items = recap_full["recap_summary"]
        elif "recap" in recap_full and isinstance(recap_full["recap"], dict) and "previous" in recap_full["recap"]:
            items = recap_full["recap"]["previous"]
            if isinstance(items, dict):
                items = [items]
        else:
            items = []
    elif isinstance(recap_full, list):
        items = recap_full
    else:
        items = []

    out = []
    for it in items:
        result = (it.get("result") or "").lower()
        if "hit" in result:
            icon = ICON_HIT
        elif "miss" in result:
            icon = ICON_MISS
        else:
            icon = ICON_NEUTRAL
        out.append({
            "player": it.get("player") or it.get("name"),
            "team": it.get("team"),
            "position": it.get("position"),
            "result": f"{icon} {it.get('result', '').strip() or 'Result N/A'}"
        })
    return out

# ---------- Main ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()

    date = args.date
    yday = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    structured_fp = os.path.join(STRUCTURED_DIR, f"enhanced_structured_players_{date}.json")
    recap_dir = os.getenv("RECAP_DIR", DEFAULT_RECAP_DIR)
    recap_fp = os.path.join(recap_dir, f"mlb_dfs_full_article_{yday}.json")

    print(f"📂 Loading structured file: {structured_fp}")
    if not os.path.exists(structured_fp):
        raise FileNotFoundError(f"Missing file: {structured_fp}")

    print(f"📂 Loading recap file: {recap_fp}")
    recap = {}
    if os.path.exists(recap_fp):
        try:
            recap = load_json(recap_fp)
        except Exception as e:
            print(f"⚠️ Could not parse recap file: {e}")
    else:
        print(f"⚠️ No recap file found for {yday}; continuing without recap.")

    players = load_json(structured_fp)
    matchups, filtered_players = matchup_gate(players)
    print(f"✅ Found {len(matchups)} valid matchups.")
    print(f"✅ Filtered to {len(filtered_players)} players in valid matchups.")

    candidates = []
    for name, p in filtered_players.items():
        if not p.get("team") or not p.get("opponent_team"):
            continue
        if p.get("position") == "P" and not p.get("is_probable_starter"):
            continue

        base_score = get_base_trend_score(p)
        fd = get_fd_avgs(p)
        trend_score = base_score + (fd["last3"] or 0.0) * 1.0 + (fd["last6"] or 0.0) * 0.5 + (fd["last9"] or 0.0) * 0.25

        tag_info = infer_tag(p)
        notes = generate_notes(p)

        if trend_score == 0.0 and tag_info["tag"] == "neutral" and "insufficient" in tag_info["reason"]:
            continue

        candidates.append({
            "name": name,
            "team": p.get("team"),
            "opponent": p.get("opponent_team"),
            "position": p.get("position"),
            "trend_score": round(trend_score, 2),
            "tag": tag_info["tag"],
            "icon": tag_info["icon"],
            "notes": notes,
            "trend_averages": get_fd_avgs(p),
        })

    print(f"🎯 Total candidates: {len(candidates)}")

    validated = candidates  # <- INCLUDE ALL probable SPs

    top_by_pos = defaultdict(list)
    for rec in validated:
        pos = rec["position"]
        if len(top_by_pos[pos]) < 3:
            top_by_pos[pos].append(rec)

    recap_summary = summarize_recap(recap)

    out = {
        "date": date,
        "matchups": sorted(list(matchups)),
        "num_valid_players": len(filtered_players),
        "recap_summary": recap_summary,
        "recommendations": top_by_pos,
        "status": "assembled with all probable SPs"
    }

    out_fp = os.path.join(STRUCTURED_DIR, f"mlb_dfs_article_{date}.json")
    with open(out_fp, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"✅ DFS article saved to {out_fp}")

if __name__ == "__main__":
    main()
