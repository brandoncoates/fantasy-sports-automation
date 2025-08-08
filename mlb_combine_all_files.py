#!/usr/bin/env python3
"""
mlb_combine_all_files.py

Combine raw JSON into structured_players_{DATE}.json
and append to player_game_log.jsonl (with a 'date' field!).
"""
import argparse
import json
import re
from datetime import datetime
from pathlib import Path

def normalize(text: str) -> str:
    return re.sub(r"[ .'\\-]", "", (text or "")).lower()

def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else []

def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--date", "-d",
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="YYYY-MM-DD"
    )
    p.add_argument(
        "--raw-dir", type=Path,
        default=Path("data") / "raw"
    )
    args = p.parse_args()
    date = args.date
    raw  = args.raw_dir

    # 1) load all raw JSONs
    rosters   = load_json(raw / "rosters"           / f"mlb_rosters_{date}.json")
    starters  = load_json(raw / "probable_starters" / f"mlb_probable_starters_{date}.json")
    weather   = load_json(raw / "weather"           / f"mlb_weather_{date}.json")
    odds      = load_json(raw / "betting"           / f"mlb_betting_odds_{date}.json")
    boxscores = load_json(raw / "boxscores"         / f"mlb_boxscores_{date}.json")

    # 2) build lookups
    team_map = {
        normalize(g[k]): g[k]
        for g in starters
        for k in ("home_team", "away_team")
    }

    weather_by = {}
    for w in weather:
        team = team_map.get(normalize(w["team"]), w["team"])
        prev = weather_by.get(team)
        if not prev or w["time_local"] < prev["time_local"]:
            weather_by[team] = w

    bet, matchup = {}, {}
    for o in odds:
        if o.get("bookmaker") != "FanDuel":
            continue
        h, a = o["home_team"], o["away_team"]
        info = {
            "over_under":     o.get("over_under"),
            "spread":         o.get("spread"),
            "favorite":       o.get("favorite"),
            "underdog":       o.get("underdog"),
            "implied_totals": o.get("implied_totals", {})
        }
        bet[h] = bet[a] = info
        matchup[normalize(h)] = {"opponent": a, "home_or_away": "home"}
        matchup[normalize(a)] = {"opponent": h, "home_or_away": "away"}

    for g in starters:
        for side in ("home_team", "away_team"):
            nm = g[side]
            key = normalize(nm)
            if key not in matchup:
                opp = g["away_team"] if side == "home_team" else g["home_team"]
                matchup[key] = {
                    "opponent":     opp,
                    "home_or_away": "home" if side == "home_team" else "away"
                }

    box_by = { normalize(b["player_name"]): b for b in boxscores }

    # 3) archive + build structured
    archive    = Path("player_game_log.jsonl")
    structured = Path(f"structured_players_{date}.json")
    players    = {}

    with archive.open("a", encoding="utf-8") as arch:
        for r in rosters:
            pid   = str(r["player_id"])
            name  = r["player"]
            team  = r["team"]
            canon = team_map.get(normalize(team), team)
            m     = matchup.get(normalize(canon), {})
            w     = weather_by.get(canon, {})
            bd    = bet.get(canon, {})

            # pull and prune box stats
            box = box_by.get(normalize(name), {}).copy()
            if r.get("position") not in ["P","SP","RP"]:
                for stat in ("innings_pitched","earned_runs","strikeouts_pitch","wins","quality_start"):
                    box.pop(stat, None)

            # rename for feature pipeline
            if "rbis" in box:
                box["rbi"] = box.pop("rbis")

            # starter flag
            starters_set = {
                normalize(g["home_pitcher"]) for g in starters
            } | {
                normalize(g["away_pitcher"]) for g in starters
            }
            is_starter = normalize(name) in starters_set

            # structured JSON entry
            players[name] = {
                "date":               date,
                "player_id":          pid,
                "name":               name,
                "team":               canon,
                "opponent_team":      m.get("opponent"),
                "home_or_away":       m.get("home_or_away"),
                "position":           r.get("position"),
                "handedness":         {"bats": r.get("bats"), "throws": r.get("throws")},
                "roster_status":      {
                    "status_code":       r.get("status_code"),
                    "status_description": r.get("status_description")
                },
                "is_probable_starter": is_starter,
                "starter":             is_starter,
                "weather_context":     w.get("weather", {}),
                "betting_context":     bd,
                "reddit_mentions":     0,
                "box_score":           box
            }

            # archive line (only if we have box stats)
            if box:
                arch.write(json.dumps({
                    "date":         date,
                    "player_id":    pid,
                    "name":         name,
                    "team":         canon,
                    "opponent":     m.get("opponent"),
                    "home_or_away": m.get("home_or_away"),
                    "box_score":    box,
                    "weather":      w.get("weather", {}),
                    "betting":      bd
                }) + "\n")

    # 4) write structured JSON
    structured.write_text(json.dumps(players, indent=2), encoding="utf-8")
    print(f"✅ Wrote {len(players)} players to {structured}")

if __name__ == "__main__":
    main()
