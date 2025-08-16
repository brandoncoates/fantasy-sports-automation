#!/usr/bin/env python3
"""
Fetch MLB active rosters for a given date and save locally, injecting any missing probable starters.
Now includes:
- Retry logic for team and player API calls (3 attempts with exponential backoff)
- Per-team progress logging
- Sanity check: fail if suspiciously low roster count
"""
import argparse
import json
import requests
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

from shared.normalize_name import normalize_name

MAX_RETRIES = 3
BACKOFF_BASE = 2  # exponential backoff base (2, 4, 8s)

def default_date_et():
    eastern_now = datetime.now(ZoneInfo("America/New_York"))
    return eastern_now.strftime("%Y-%m-%d")

def safe_request(url, timeout=10, retries=MAX_RETRIES, label="request"):
    """Perform a GET request with retry/backoff."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            wait = BACKOFF_BASE ** (attempt - 1)
            print(f"⚠️  {label} failed (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                print(f"   ⏳ Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"❌ {label} permanently failed after {retries} attempts")
                return None
    return None

def load_probable_starters(date_str: str, repo_root: Path):
    path = repo_root / "data" / "raw" / "probable_starters" / f"mlb_probable_starters_{date_str}.json"
    if not path.exists():
        print(f"⚠️  Probable starters file not found: {path}")
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"⚠️  Error loading probable starters: {e}")
        return []

def main():
    repo_root = Path(__file__).resolve().parent
    default_outdir = repo_root / "data" / "raw" / "rosters"

    parser = argparse.ArgumentParser(
        description="Fetch MLB active rosters for a given date and save locally."
    )
    parser.add_argument("--date", type=str, default=default_date_et(),
                        help="Date in YYYY-MM-DD format (default: today ET)")
    parser.add_argument("--outdir", type=Path, default=default_outdir,
                        help="Output directory for JSON files")
    args = parser.parse_args()

    target_date = args.date
    outdir: Path = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)
    local_path = outdir / f"mlb_rosters_{target_date}.json"

    # === STEP 1: Fetch teams list ===
    teams_url = "https://statsapi.mlb.com/api/v1/teams?sportId=1"
    resp = safe_request(teams_url, timeout=10, label="fetch teams list")
    if not resp:
        raise RuntimeError("Could not fetch teams list from MLB StatsAPI")
    teams = resp.json().get("teams", [])

    # === STEP 2: Fetch active rosters ===
    records = []
    for idx, team in enumerate(teams, start=1):
        team_id = team.get("id")
        team_name = team.get("name")
        print(f"➡️  [{idx}/{len(teams)}] Fetching roster for {team_name}...")
        roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active"
        r = safe_request(roster_url, timeout=10, label=f"{team_name} roster")
        if not r:
            continue
        roster = r.json().get("roster", [])

        for player in roster:
            person = player.get("person", {})
            raw_name = person.get("fullName", "")
            name = normalize_name(raw_name)
            player_id = person.get("id", "")
            position = player.get("position", {}).get("abbreviation", "")
            status = player.get("status", {})
            status_code = status.get("code", "")
            status_desc = status.get("description", "")

            bats = throws = None
            details_url = f"https://statsapi.mlb.com/api/v1/people/{player_id}"
            d = safe_request(details_url, timeout=5, retries=2, label=f"{name} details")
            if d:
                try:
                    info = d.json().get("people", [{}])[0]
                    bats = info.get("batSide", {}).get("code", "")
                    throws = info.get("pitchHand", {}).get("code", "")
                except Exception:
                    pass

            records.append({
                "date":               target_date,
                "team":               team_name,
                "player":             name,
                "player_id":          player_id,
                "position":           position,
                "status_code":        status_code,
                "status_description": status_desc,
                "bats":               bats,
                "throws":             throws
            })

    # === STEP 3: Inject missing probable starters ===
    starters = load_probable_starters(target_date, repo_root)
    roster_names = {rec["player"].lower() for rec in records}
    for game in starters:
        for role in ("home_pitcher", "away_pitcher"):
            nm = game.get(role, "").strip()
            if not nm or nm.lower() in roster_names:
                continue
            team = game.get("home_team") if role == "home_pitcher" else game.get("away_team")
            print(f"➕ Injecting probable starter: {nm}")
            records.append({
                "date":               target_date,
                "team":               team,
                "player":             nm,
                "player_id":          f"manual-{normalize_name(nm)}",
                "position":           "P",
                "status_code":        "A",
                "status_description": "Probable Starter (Injected)",
                "bats":               None,
                "throws":             None
            })

    # === STEP 4: Debug & save ===
    total = len(records)
    print(f"🔍 Rosters scraped: {total} entries")
    if total < 740:
        raise RuntimeError(f"Roster count too low ({total}). Incomplete scrape, not saving {local_path}")
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    print(f"💾 Saved rosters to {local_path} ({total} records)")

if __name__ == "__main__":
    main()
