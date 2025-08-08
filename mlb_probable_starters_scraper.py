#!/usr/bin/env python3
"""
Fetch MLB probable starters from MLB API for a given date and save locally.
"""
import argparse
import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

from shared.normalize_name import normalize_name

def default_date_et():
    eastern_now = datetime.now(ZoneInfo("America/New_York"))
    return eastern_now.strftime("%Y-%m-%d")

def get_throw_hand(player_id: int) -> str:
    if not player_id:
        return ""
    try:
        resp = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{player_id}", timeout=5
        )
        resp.raise_for_status()
        info = resp.json().get("people", [{}])[0]
        return info.get("pitchHand", {}).get("code", "")
    except requests.RequestException:
        return ""

def main():
    repo_root = Path(__file__).resolve().parent
    default_outdir = repo_root / "data" / "raw" / "probable_starters"

    parser = argparse.ArgumentParser(
        description="Fetch MLB probable starters from MLB API for a given date and save locally."
    )
    parser.add_argument(
        "--date", type=str, default=default_date_et(),
        help="Date in YYYY-MM-DD format (default: today ET)"
    )
    parser.add_argument(
        "--outdir", type=Path, default=default_outdir,
        help="Output directory for JSON files"
    )
    args = parser.parse_args()

    target_date = args.date
    outdir: Path = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)
    local_path = outdir / f"mlb_probable_starters_{target_date}.json"

    schedule_url = (
        f"https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&date={target_date}&hydrate=probablePitcher"
    )
    try:
        resp = requests.get(schedule_url, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Error fetching schedule: {e}")
        return

    dates = resp.json().get("dates", [])
    if not dates or not dates[0].get("games"):
        print(f"⚠️ No MLB games scheduled for {target_date}. Exiting.")
        return

    records = []
    for game in dates[0]["games"]:
        gid = game.get("gamePk")
        home_info = game["teams"]["home"]
        away_info = game["teams"]["away"]

        raw_home = home_info.get("probablePitcher", {}).get("fullName", "")
        raw_away = away_info.get("probablePitcher", {}).get("fullName", "")
        home_nm = normalize_name(raw_home)
        away_nm = normalize_name(raw_away)

        home_id = home_info.get("probablePitcher", {}).get("id")
        away_id = away_info.get("probablePitcher", {}).get("id")
        home_hd = get_throw_hand(home_id)
        away_hd = get_throw_hand(away_id)

        records.append({
            "date":            target_date,
            "game_id":         gid,
            "game_datetime":   game.get("gameDate"),
            "away_team":       away_info["team"]["name"],
            "away_pitcher":    away_nm,
            "away_throw_hand": away_hd,
            "home_team":       home_info["team"]["name"],
            "home_pitcher":    home_nm,
            "home_throw_hand": home_hd
        })

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    print(f"💾 Saved probable starters to {local_path} ({len(records)} records)")

if __name__ == "__main__":
    main()
