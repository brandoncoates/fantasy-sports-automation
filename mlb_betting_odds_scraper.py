#!/usr/bin/env python3
"""
Fetch MLB betting odds from FanDuel for a given date and save locally.
"""
import os
import argparse
import requests
import json
from datetime import datetime
import re
from pathlib import Path

# === TEAM NAME NORMALIZATION MAP ===
TEAM_NAME_MAP = {
    # … same mapping as before …
}

def normalize(name: str) -> str:
    return re.sub(r"[ .'\-]", "", name.lower())

def main():
    # compute repo_root, then default under data/raw/betting
    repo_root = Path(__file__).resolve().parent
    default_outdir = repo_root / "data" / "raw" / "betting"

    parser = argparse.ArgumentParser(
        description="Fetch MLB betting odds from FanDuel for a given date and save locally."
    )
    parser.add_argument(
        "--date", type=str, default=datetime.now().strftime("%Y-%m-%d"),
        help="Date in YYYY-MM-DD format (default: today)"
    )
    parser.add_argument(
        "--outdir", type=Path, default=default_outdir,
        help="Output directory for JSON files"
    )
    parser.add_argument(
        "--api-key", type=str,
        help="Odds API key (overrides env var)"
    )
    parser.add_argument(
        "--api-key-env", type=str, default="ODDS_API_KEY",
        help="Name of the environment variable containing the Odds API key"
    )
    parser.add_argument(
        "--bookmakers", type=str, default="fanduel",
        help="Comma-separated list of bookmakers (default: fanduel)"
    )
    parser.add_argument(
        "--markets", type=str, default="totals,spreads",
        help="Comma-separated list of markets (default: totals,spreads)"
    )
    args = parser.parse_args()

    api_key = args.api_key or os.getenv(args.api_key_env)
    if not api_key:
        parser.error(f"You must provide an API key via --api-key or set {args.api_key_env}")

    target_date = args.date
    outdir: Path = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)
    local_path = outdir / f"mlb_betting_odds_{target_date}.json"

    print(f"📡 Requesting MLB betting odds for {target_date}…")
    try:
        resp = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds",
            params={
                "apiKey": api_key,
                "regions": "us",
                "markets": args.markets,
                "bookmakers": args.bookmakers,
                "oddsFormat": "decimal",
                "dateFormat": "iso",
            },
            timeout=10
        )
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching odds: {e}")
        return

    games = resp.json()
    if not games:
        print("⚠️ No odds returned — exiting.")
        return

    results = []
    for game in games:
        home = TEAM_NAME_MAP.get(normalize(game.get("home_team","")), game.get("home_team",""))
        away = TEAM_NAME_MAP.get(normalize(game.get("away_team","")), game.get("away_team",""))
        time_str = game.get("commence_time","")[:19].replace("T"," ")

        for book in game.get("bookmakers", []):
            if book.get("title","").lower() != "fanduel":
                continue

            over_under = spread = None
            favorite = underdog = None
            implied_totals = {}

            for market in book.get("markets", []):
                key = market.get("key")
                for outcome in market.get("outcomes", []):
                    name = outcome.get("name","")
                    point = outcome.get("point")
                    if key=="totals" and name.lower()=="over" and point is not None:
                        over_under = point
                    elif key=="spreads" and point is not None:
                        if name==home and point<0:
                            favorite, underdog, spread = home, away, abs(point)
                        elif name==away and point<0:
                            favorite, underdog, spread = away, home, abs(point)
                    elif key=="team_totals" and point is not None:
                        canon = TEAM_NAME_MAP.get(normalize(name), name)
                        implied_totals[canon] = point

            results.append({
                "date":           target_date,
                "time":           time_str,
                "bookmaker":      book.get("title",""),
                "home_team":      home,
                "away_team":      away,
                "over_under":     over_under,
                "spread":         spread,
                "favorite":       favorite,
                "underdog":       underdog,
                "implied_totals": implied_totals
            })

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"💾 Saved betting odds to {local_path} ({len(results)} games)")

if __name__ == "__main__":
    main()
