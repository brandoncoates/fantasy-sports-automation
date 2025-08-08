#!/usr/bin/env python3
"""
Fetch MLB box score stats for a given date and save locally.
"""
import argparse
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import re
import requests
from pathlib import Path

# Assume shared.normalize_name exists in repo
from shared.normalize_name import normalize_name

# Helpers to coerce stats into numbers

def to_int(val):
    try:
        return int(val)
    except Exception:
        return 0


def to_float(val):
    try:
        return float(val)
    except Exception:
        return 0.0


def default_date_et():
    """Return yesterday's date string in Eastern Time."""
    eastern_now = datetime.now(ZoneInfo("America/New_York"))
    return (eastern_now - timedelta(days=1)).strftime("%Y-%m-%d")


def main():
    project_root = Path(__file__).resolve().parent
    default_outdir = project_root / "data" / "raw" / "boxscores"

    parser = argparse.ArgumentParser(
        description="Fetch MLB box scores from MLB API for a given date and save locally."
    )
    parser.add_argument(
        "--date", type=str, default=default_date_et(),
        help="Date in YYYY-MM-DD format (default: yesterday ET)"
    )
    parser.add_argument(
        "--outdir", type=Path, default=default_outdir,
        help="Output directory for JSON files"
    )
    args = parser.parse_args()

    target_date = args.date
    outdir: Path = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)
    filename = f"mlb_boxscores_{target_date}.json"
    local_path = outdir / filename

    # === STEP 1: Fetch schedule ===
    schedule_url = (
        f"https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&date={target_date}"
    )
    try:
        resp = requests.get(schedule_url, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching schedule: {e}")
        return

    dates = resp.json().get("dates", [])
    games = dates[0].get("games", []) if dates else []
    game_ids = [g.get("gamePk") for g in games]
    print(f"🔎 Found {len(game_ids)} games for {target_date}")

    # === STEP 2: Fetch boxscores ===
    records = []
    for game_id in game_ids:
        try:
            box = requests.get(
                f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore",
                timeout=10
            )
            box.raise_for_status()
            data = box.json()
            for side in ("home", "away"):
                team_name = data["teams"][side]["team"]["name"]
                players = data["teams"][side]["players"].values()

                for player in players:
                    raw_name = player["person"].get("fullName", "")
                    name = normalize_name(raw_name)

                    bat = player.get("stats", {}).get("batting", {})
                    pit = player.get("stats", {}).get("pitching", {})

                    ip = to_float(pit.get("inningsPitched"))
                    er = to_int(pit.get("earnedRuns"))
                    qs = 1 if (ip >= 6 and er <= 3) else 0

                    records.append({
                        "game_date": target_date,
                        "game_id": game_id,
                        "team": team_name,
                        "player_name": name,
                        "position": ", ".join(
                            pos.get("abbreviation", "") 
                            for pos in player.get("allPositions", [])
                        ),
                        # batting
                        "at_bats": to_int(bat.get("atBats")),
                        "runs": to_int(bat.get("runs")),
                        "hits": to_int(bat.get("hits")),
                        "doubles": to_int(bat.get("doubles")),
                        "triples": to_int(bat.get("triples")),
                        "home_runs": to_int(bat.get("homeRuns")),
                        "rbis": to_int(bat.get("rbi")),
                        "walks": to_int(bat.get("baseOnBalls")),
                        "strikeouts_bat": to_int(bat.get("strikeOuts")),
                        "stolen_bases": to_int(bat.get("stolenBases")),
                        # pitching
                        "innings_pitched": ip,
                        "earned_runs": er,
                        "strikeouts_pitch": to_int(pit.get("strikeOuts")),
                        "wins": to_int(pit.get("wins")),
                        "quality_start": qs
                    })
        except requests.exceptions.RequestException as e:
            print(f"❌ Error for game {game_id}: {e}")
            continue
        except Exception as e:
            print(f"❌ Skipped game {game_id} due to: {e}")
            continue

    # === STEP 3: Save locally ===
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    print(f"💾 Saved box scores to {local_path} ({len(records)} records)")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Fetch MLB box score stats for a given date and save locally.
"""
import argparse
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
from pathlib import Path

# Assume shared.normalize_name exists in repo
from shared.normalize_name import normalize_name

# Helpers to coerce stats into numbers

def to_int(val):
    try:
        return int(val)
    except Exception:
        return 0


def to_float(val):
    try:
        return float(val)
    except Exception:
        return 0.0


def default_date_et():
    """Return yesterday's date string in Eastern Time."""
    eastern_now = datetime.now(ZoneInfo("America/New_York"))
    return (eastern_now - timedelta(days=1)).strftime("%Y-%m-%d")


def main():
    # Write to the repo-level data/raw/boxscores directory by default
    default_outdir = Path.cwd() / "data" / "raw" / "boxscores"

    parser = argparse.ArgumentParser(
        description="Fetch MLB box scores from MLB API for a given date and save locally."
    )
    parser.add_argument(
        "--date", type=str, default=default_date_et(),
        help="Date in YYYY-MM-DD format (default: yesterday ET)"
    )
    parser.add_argument(
        "--outdir", type=Path, default=default_outdir,
        help="Output directory for JSON files"
    )
    args = parser.parse_args()

    target_date = args.date
    outdir: Path = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)
    filename = f"mlb_boxscores_{target_date}.json"
    local_path = outdir / filename

    # === STEP 1: Fetch schedule ===
    schedule_url = (
        f"https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&date={target_date}"
    )
    try:
        resp = requests.get(schedule_url, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching schedule: {e}")
        return

    dates = resp.json().get("dates", [])
    games = dates[0].get("games", []) if dates else []
    game_ids = [g.get("gamePk") for g in games]
    print(f"🔎 Found {len(game_ids)} games for {target_date}")

    # === STEP 2: Fetch boxscores ===
    records = []
    for game_id in game_ids:
        try:
            box = requests.get(
                f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore",
                timeout=10
            )
            box.raise_for_status()
            data = box.json()
            for side in ("home", "away"):
                team_name = data["teams"][side]["team"]["name"]
                players = data["teams"][side]["players"].values()

                for player in players:
                    raw_name = player["person"].get("fullName", "")
                    name = normalize_name(raw_name)

                    bat = player.get("stats", {}).get("batting", {})
                    pit = player.get("stats", {}).get("pitching", {})

                    ip = to_float(pit.get("inningsPitched"))
                    er = to_int(pit.get("earnedRuns"))
                    qs = 1 if (ip >= 6 and er <= 3) else 0

                    records.append({
                        "game_date": target_date,
                        "game_id": game_id,
                        "team": team_name,
                        "player_name": name,
                        "position": ", ".join(
                            pos.get("abbreviation", "") 
                            for pos in player.get("allPositions", [])
                        ),
                        # batting
                        "at_bats": to_int(bat.get("atBats")),
                        "runs": to_int(bat.get("runs")),
                        "hits": to_int(bat.get("hits")),
                        "doubles": to_int(bat.get("doubles")),
                        "triples": to_int(bat.get("triples")),
                        "home_runs": to_int(bat.get("homeRuns")),
                        "rbis": to_int(bat.get("rbi")),
                        "walks": to_int(bat.get("baseOnBalls")),
                        "strikeouts_bat": to_int(bat.get("strikeOuts")),
                        "stolen_bases": to_int(bat.get("stolenBases")),
                        # pitching
                        "innings_pitched": ip,
                        "earned_runs": er,
                        "strikeouts_pitch": to_int(pit.get("strikeOuts")),
                        "wins": to_int(pit.get("wins")),
                        "quality_start": qs
                    })
        except requests.exceptions.RequestException as e:
            print(f"❌ Error for game {game_id}: {e}")
            continue
        except Exception as e:
            print(f"❌ Skipped game {game_id} due to: {e}")
            continue

    # === STEP 3: Save locally ===
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    print(f"💾 Saved box scores to {local_path} ({len(records)} records)")


if __name__ == "__main__":
    main()
