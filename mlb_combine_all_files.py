#!/usr/bin/env python3
"""
mlb_combine_all_files.py

Combine raw JSON into structured_players_{DATE}.json
and append to player_game_log.jsonl (with a 'date' field).
- Expects all inputs under data/raw/<category>/..._{DATE}.json
- Box scores are saved as ..._{DATE}.json even if they represent yesterday's games.
"""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path


def normalize(text: str) -> str:
    """Lowercase and strip common punctuation/spaces for matching."""
    return re.sub(r"[ .'\-]", "", (text or "")).lower()


def load_json(path: Path):
    """Safe JSON loader: return [] if file missing or unreadable."""
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--date", "-d",
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Pipeline date YYYY-MM-DD (all inputs should use this filename date)."
    )
    p.add_argument(
        "--raw-dir", type=Path,
        default=Path("data") / "raw",
        help="Root folder containing raw json folders (rosters, probable_starters, weather, betting, boxscores)."
    )
    args = p.parse_args()

    date = args.date
    raw = args.raw_dir

    # --- 1) Load raw inputs -----------------------------------------------
    rosters   = load_json(raw / "rosters"            / f"mlb_rosters_{date}.json")
    starters  = load_json(raw / "probable_starters"  / f"mlb_probable_starters_{date}.json")
    weather   = load_json(raw / "weather"            / f"mlb_weather_{date}.json")
    odds      = load_json(raw / "betting"            / f"mlb_betting_odds_{date}.json")
    boxscores = load_json(raw / "boxscores"          / f"mlb_boxscores_{date}.json")

    print(
        f"🔍 Loaded: rosters={len(rosters)}, starters={len(starters)}, "
        f"weather={len(weather)}, odds={len(odds)}, boxscores={len(boxscores)}"
    )

    # --- 2) Build lookups --------------------------------------------------
    # Map normalized team -> canonical team name using starters (most reliable daily list)
    team_map = {
        normalize(g[k]): g[k]
        for g in starters
        for k in ("home_team", "away_team")
    }

    # Earliest weather per team (by time_local), keyed by canonical team
    weather_by = {}
    for w in weather:
        team_canon = team_map.get(normalize(w.get("team", "")), w.get("team", ""))
        prev = weather_by.get(team_canon)
        if not prev or w.get("time_local", "") < prev.get("time_local", ""):
            weather_by[team_canon] = w

    # Betting info per team + matchup map (home/away + opponent)
    bet, matchup = {}, {}
    for o in odds:
        if str(o.get("bookmaker", "")).lower() != "fanduel":
            continue
        h, a = o.get("home_team"), o.get("away_team")
        if not h or not a:
            continue
        info = {
            "over_under":     o.get("over_under"),
            "spread":         o.get("spread"),
            "favorite":       o.get("favorite"),
            "underdog":       o.get("underdog"),
            "implied_totals": o.get("implied_totals", {}) or {},
        }
        bet[h] = bet[a] = info
        matchup[normalize(h)] = {"opponent": a, "home_or_away": "home"}
        matchup[normalize(a)] = {"opponent": h, "home_or_away": "away"}

    # Ensure every starters team has a matchup entry (even if odds missing)
    for g in starters:
        for side in ("home_team", "away_team"):
            t_name = g.get(side, "")
            key = normalize(t_name)
            if key not in matchup:
                opp = g.get("away_team") if side == "home_team" else g.get("home_team")
                matchup[key] = {
                    "opponent": opp,
                    "home_or_away": "home" if side == "home_team" else "away"
                }

    # Box scores by normalized player name
    box_by = {normalize(b.get("player_name", "")): b for b in boxscores}

    # Precompute starters set for quick lookup
    starters_set = (
        {normalize(g.get("home_pitcher", "")) for g in starters}
        | {normalize(g.get("away_pitcher", "")) for g in starters}
    )

    # --- 3) Write archive + build structured -------------------------------
    archive_path = Path("player_game_log.jsonl")
    structured_path = Path(f"structured_players_{date}.json")
    players = {}

    with archive_path.open("a", encoding="utf-8") as arch:
        for r in rosters:
            pid = str(r.get("player_id", ""))
            name = r.get("player", "")
            team = r.get("team", "")

            canon_team = team_map.get(normalize(team), team)
            m = matchup.get(normalize(canon_team), {})
            wctx_full = weather_by.get(canon_team, {})
            bd = bet.get(canon_team, {}) or {}

            # Player box line (copy to avoid mutating original)
            box = box_by.get(normalize(name), {}).copy()
            if r.get("position") not in {"P", "SP", "RP"}:
                for stat in ("innings_pitched", "earned_runs", "strikeouts_pitch", "wins", "quality_start"):
                    box.pop(stat, None)

            # Rename for consistency if present
            if "rbis" in box:
                box["rbi"] = box.pop("rbis")

            # Flag probable starter
            is_starter = normalize(name) in starters_set

            # Single structured entry per player (keyed by player name)
            players[name] = {
                "date":               date,                           # pipeline date
                "game_date":          box.get("game_date", date),     # actual game date (from box, if present)
                "player_id":          pid,
                "name":               name,
                "team":               canon_team,
                "opponent_team":      m.get("opponent"),
                "home_or_away":       m.get("home_or_away"),
                "position":           r.get("position"),
                "handedness":         {"bats": r.get("bats"), "throws": r.get("throws")},
                "roster_status":      {
                    "status_code":        r.get("status_code"),
                    "status_description": r.get("status_description")
                },
                "is_probable_starter": is_starter,
                "starter":             is_starter,
                "weather_context":     (wctx_full.get("weather", {}) if isinstance(wctx_full, dict) else {}) or {},
                "betting_context":     bd,
                "reddit_mentions":     0,
                "box_score":           box,
            }

            # Append to archive only if we have a box score line
            if box:
                arch.write(json.dumps({
                    "date":         date,                       # keep pipeline date for analyzer joins
                    "player_id":    pid,
                    "name":         name,
                    "team":         canon_team,
                    "opponent":     m.get("opponent"),
                    "home_or_away": m.get("home_or_away"),
                    "box_score":    box,
                    "weather":      (wctx_full.get("weather", {}) if isinstance(wctx_full, dict) else {}) or {},
                    "betting":      bd,
                }) + "\n")

    # --- 4) Write structured file + quick schema echo ----------------------
    structured_path.write_text(json.dumps(players, indent=2), encoding="utf-8")
    print(f"✅ Wrote {len(players)} players to {structured_path}")

    try:
        data = json.loads(structured_path.read_text(encoding="utf-8"))
        if data:
            first = next(iter(data.values()))
            print("🔑 Structured JSON keys:", list(first.keys()))
        else:
            print("⚠️ Structured JSON is empty!")
    except Exception as e:
        print(f"⚠️ Unable to re-read structured JSON: {e}")


if __name__ == "__main__":
    main()
