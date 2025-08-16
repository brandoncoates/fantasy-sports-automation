#!/usr/bin/env python3
"""
Fetch MLB box score stats for (pipeline_date - 1 day) and save as
data/raw/boxscores/mlb_boxscores_<pipeline_date>.json

So if you pass --date 2025-08-08, it fetches games for 2025-08-07 but writes
mlb_boxscores_2025-08-08.json (keeps combine happy).
"""
import argparse
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import requests
from shared.normalize_name import normalize_name

def to_int(x):
    try: return int(x)
    except: return 0

def to_float(x):
    try: return float(x)
    except: return 0.0

def main():
    repo_root = Path.cwd()
    outdir = repo_root / "data" / "raw" / "boxscores"

    p = argparse.ArgumentParser(description="Fetch MLB box scores for (date - 1 day).")
    p.add_argument("--date", required=True, help="Pipeline date YYYY-MM-DD (we fetch date-1).")
    p.add_argument("--outdir", type=Path, default=outdir, help="Output directory")
    args = p.parse_args()

    pipeline_date_str = args.date
    # interpret pipeline date in Eastern; scrape yesterday (ET)
    et = ZoneInfo("America/New_York")
    pipeline_dt = datetime.strptime(pipeline_date_str, "%Y-%m-%d").replace(tzinfo=et)
    scrape_dt = pipeline_dt
    scrape_date_str = scrape_dt.strftime("%Y-%m-%d")

    args.outdir.mkdir(parents=True, exist_ok=True)
    # Save using the pipeline date so combine reads mlb_boxscores_<DATE>.json
    out_path = args.outdir / f"mlb_boxscores_{pipeline_date_str}.json"

    print(f"📅 Pipeline date: {pipeline_date_str}")
    print(f"🕒 Scraping box scores for yesterday (ET): {scrape_date_str}")
    print(f"💾 Will save to: {out_path}")

    # 1) schedule for scrape_date
    sched_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={scrape_date_str}"
    try:
        r = requests.get(sched_url, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Error fetching schedule: {e}")
        return

    dates = r.json().get("dates", [])
    games = dates[0].get("games", []) if dates else []
    game_ids = [g.get("gamePk") for g in games]
    print(f"🔎 Found {len(game_ids)} games for {scrape_date_str}")

    # 2) box scores
    records = []
    for gid in game_ids:
        try:
            box = requests.get(f"https://statsapi.mlb.com/api/v1/game/{gid}/boxscore", timeout=15)
            box.raise_for_status()
            data = box.json()

            for side in ("home", "away"):
                team_name = data["teams"][side]["team"]["name"]
                for player in data["teams"][side]["players"].values():
                    raw = player["person"].get("fullName", "")
                    name = normalize_name(raw)

                    bat = player.get("stats", {}).get("batting", {})
                    pit = player.get("stats", {}).get("pitching", {})

                    ip = to_float(pit.get("inningsPitched"))
                    er = to_int(pit.get("earnedRuns"))
                    qs = 1 if (ip >= 6 and er <= 3) else 0

                    records.append({
                        "game_date": scrape_date_str,    # yesterday (ET)
                        "game_id": gid,
                        "team": team_name,
                        "player_name": name,
                        "position": ", ".join(pos.get("abbreviation","")
                                              for pos in player.get("allPositions", [])),
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
                        "quality_start": qs,
                    })
        except requests.RequestException as e:
            print(f"❌ Boxscore fetch failed for game {gid}: {e}")
        except Exception as e:
            print(f"❌ Skipped game {gid} due to: {e}")

    out_path.write_text(json.dumps(records, indent=2), encoding="utf-8")
    print(f"✅ Saved {len(records)} box-score records to {out_path}")

if __name__ == "__main__":
    main()
