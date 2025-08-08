#!/usr/bin/env python3
"""
mlb_combine_all_files.py

Combine raw MLB JSON outputs into one structured per-player JSON,
and append each game entry to an append-only archive for trend analysis.
"""
import argparse
import json
import glob
import re
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

def normalize(text: str) -> str:
    """Normalize text by stripping punctuation/spaces and lowercasing."""
    return re.sub(r"[ .'\\-]", "", (text or "")).lower()

def load_json(path: Path):
    if not path.exists():
        print(f"⚠️  {path} not found — skipping.")
        return []
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser(
        description="Combine raw MLB JSON files into structured player output."
    )
    parser.add_argument(
        "--date", "-d",
        help="Date in YYYY-MM-DD format (default: today UTC)",
        default=datetime.utcnow().strftime("%Y-%m-%d")
    )
    parser.add_argument(
        "--raw-dir",
        help="Root folder where raw JSON lives",
        type=Path,
        default=Path("data") / "raw"
    )
    args = parser.parse_args()

    date_str = args.date
    yday_str = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    raw = args.raw_dir

    # load each source
    rosters = load_json(raw / "rosters" / f"mlb_rosters_{date_str}.json")
    starters = load_json(raw / "probable_starters" / f"mlb_probable_starters_{date_str}.json")
    weather = load_json(raw / "weather" / f"mlb_weather_{date_str}.json")
    odds    = load_json(raw / "betting" / f"mlb_betting_odds_{date_str}.json")
    boxscores = load_json(raw / "boxscores" / f"mlb_boxscores_{yday_str}.json")

    # build team lookup from starters
    team_map = {}
    for g in starters:
        for side in ("home_team", "away_team"):
            raw_team = g.get(side, "")
            team_map[normalize(raw_team)] = raw_team

    # WEATHER: earliest per team
    weather_by_team = {}
    for w in weather:
        team = w.get("team", "")
        canon = team_map.get(normalize(team), team)
        prev = weather_by_team.get(canon)
        if not prev or w.get("time_local", "") < prev.get("time_local", ""):
            weather_by_team[canon] = w

    # BETTING + matchups
    bet_by_team = {}
    matchup_by_team = {}
    for o in odds:
        if o.get("bookmaker") != "FanDuel":
            continue
        h = o.get("home_team", "")
        a = o.get("away_team", "")
        if h and a:
            bet_info = {
                "over_under": o.get("over_under"),
                "spread": o.get("spread"),
                "favorite": o.get("favorite"),
                "underdog": o.get("underdog"),
                "implied_totals": o.get("implied_totals", {})
            }
            bet_by_team[h] = bet_info
            bet_by_team[a] = bet_info
            matchup_by_team[normalize(h)] = {"opponent": a, "home_or_away": "home"}
            matchup_by_team[normalize(a)] = {"opponent": h, "home_or_away": "away"}

    # ensure every starter has a matchup
    for g in starters:
        h = g.get("home_team",""); a = g.get("away_team","")
        if normalize(h) not in matchup_by_team:
            matchup_by_team[normalize(h)] = {"opponent": a, "home_or_away": "home"}
        if normalize(a) not in matchup_by_team:
            matchup_by_team[normalize(a)] = {"opponent": h, "home_or_away": "away"}

    # box scores by normalized player name
    box_by_name = {
        normalize(b.get("player_name","")): b
        for b in boxscores
    }

    # prepare archive and output
    archive_path = Path("player_game_log.jsonl")
    out_file = Path(f"structured_players_{date_str}.json")
    players_out = {}

    # append to archive per player-game
    with archive_path.open("a", encoding="utf-8") as archive:
        for r in rosters:
            pid = str(r.get("player_id",""))
            name = r.get("player","")
            team = r.get("team","")
            canon_team = team_map.get(normalize(team), team)
            matchup = matchup_by_team.get(normalize(canon_team), {})
            bet = bet_by_team.get(canon_team, {})
            wc = weather_by_team.get(canon_team, {})

            # fetch box score, drop pitching fields for non-pitchers
            box = box_by_name.get(normalize(name), {}).copy()
            if r.get("position") not in ["P","SP","RP"]:
                for stat in ["innings_pitched","earned_runs","strikeouts_pitch","wins","quality_start"]:
                    box.pop(stat, None)

            # is_probable_starter?
            starter_names = {
                normalize(g.get("home_pitcher","")) for g in starters
            } | {
                normalize(g.get("away_pitcher","")) for g in starters
            }
            is_starter = normalize(name) in starter_names

            # build structured entry
            players_out[name] = {
                "player_id": pid,
                "name": name,
                "team": canon_team,
                "opponent_team": matchup.get("opponent"),
                "home_or_away": matchup.get("home_or_away"),
                "position": r.get("position",""),
                "handedness": {"bats": r.get("bats"), "throws": r.get("throws")},
                "roster_status": {
                    "status_code": r.get("status_code"),
                    "status_description": r.get("status_description")
                },
                "is_probable_starter": is_starter,
                "starter": is_starter,
                "weather_context": wc.get("weather", {}),
                "betting_context": bet,
                "espn_mentions": 0,
                "espn_articles": [],
                "reddit_mentions": 0,
                "box_score": box,
            }

            # append archive entry if box present
            if box:
                entry = {
                    "date": yday_str,
                    "player_id": pid,
                    "name": name,
                    "team": canon_team,
                    "opponent": matchup.get("opponent"),
                    "home_or_away": matchup.get("home_or_away"),
                    "box_score": box,
                    "weather": wc.get("weather", {}),
                    "betting": bet,
                }
                archive.write(json.dumps(entry) + "\n")

    # write structured JSON
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(players_out, f, indent=2)
    print(f"✅ Wrote {len(players_out)} players to {out_file}")

if __name__ == "__main__":
    main()
