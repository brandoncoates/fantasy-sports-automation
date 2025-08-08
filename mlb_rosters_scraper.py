#!/usr/bin/env python3
"""
Fetch MLB active rosters for a given date and save locally, injecting any missing probable starters.
"""
import argparse
import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

from shared.normalize_name import normalize_name


def default_date_et():
    """Return today's date string in Eastern Time."""
    eastern_now = datetime.now(ZoneInfo("America/New_York"))
    return eastern_now.strftime("%Y-%m-%d")


def load_probable_starters(date_str: str):
    """Load probable starters from local JSON, if available."""
    path = (
        Path.cwd()
        / "data"
        / "raw"
        / "probable_starters"
        / f"mlb_probable_starters_{date_str}.json"
    )
    if not path.exists():
        print(f"⚠️  Probable starters file not found: {path}")
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"⚠️  Error loading probable starters: {e}")
        return []


def main():
    # default to repo-root/data/raw/rosters
    default_outdir = Path.cwd() / "data" / "raw" / "rosters"

    parser = argparse.ArgumentParser(
        description="Fetch MLB active rosters for a given date and save locally."
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
    filename = f"mlb_rosters_{target_date}.json"
    local_path = outdir / filename

    # === STEP 1: Fetch teams list ===
    teams_url = "https://statsapi.mlb.com/api/v1/teams?sportId=1"
    try:
        resp = requests.get(teams_url, timeout=10)
        resp.raise_for_status()
        teams = resp.json().get("teams", [])
    except requests.RequestException as e:
        print(f"❌ Error fetching teams: {e}")
        return

    # === STEP 2: Fetch active rosters ===
    records = []
    for team in teams:
        team_id = team.get("id")
        team_name = team.get("name")
        roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active"
        try:
            r = requests.get(roster_url, timeout=10)
            r.raise_for_status()
            roster = r.json().get("roster", [])
        except requests.RequestException as e:
            print(f"❌ Failed to fetch roster for {team_name}: {e}")
            continue

        for player in roster:
            person = player.get("person", {})
            raw_name = person.get("fullName", "")
            name = normalize_name(raw_name)
            player_id = person.get("id", "")
            position = player.get("position", {}).get("abbreviation", "")
            status = player.get("status", {})
            status_code = status.get("code", "")
            status_desc = status.get("description", "")

            # Fetch hand info
            bats = throws = None
            details_url = f"https://statsapi.mlb.com/api/v1/people/{player_id}"
            try:
                d = requests.get(details_url, timeout=5)
                d.raise_for_status()
                info = d.json().get("people", [{}])[0]
                bats = info.get("batSide", {}).get("code", "")
                throws = info.get("pitchHand", {}).get("code", "")
            except requests.RequestException:
                pass

            records.append({
                "date": target_date,
                "team": team_name,
                "player": name,
                "player_id": player_id,
                "position": position,
                "status_code": status_code,
                "status_description": status_desc,
                "bats": bats,
                "throws": throws
            })

    # === STEP 3: Inject missing probable starters ===
    starters = load_probable_starters(target_date)
    roster_names = {rec["player"].lower() for rec in records}
    for game in starters:
        for role in ("home_pitcher", "away_pitcher"):
            nm = game.get(role, "").strip()
            if not nm or nm.lower() in roster_names:
                continue
            team = game.get("home_team") if role == "home_pitcher" else game.get("away_team")
            print(f"➕ Injecting probable starter: {nm}")
            records.append({
                "date": target_date,
                "team": team,
                "player": nm,
                "player_id": f"manual-{normalize_name(nm)}",
                "position": "P",
                "status_code": "A",
                "status_description": "Probable Starter (Injected)",
                "bats": None,
                "throws": None
            })

    print(f"✅ Final roster count: {len(records)} players.")

    # === STEP 4: Save locally ===
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    print(f"💾 Saved rosters to {local_path} ({len(records)} records)")


if __name__ == "__main__":
    main()
