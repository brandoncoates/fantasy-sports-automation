#!/usr/bin/env python3
"""
Fetch MLB game-time weather for a given date’s probable starters and save locally.
"""
import argparse
import json
import time
import requests
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import pandas as pd

# Helpers

def normalize_key(text: str) -> str:
    """Normalize text to match team keys (lowercase, no punctuation)."""
    return re.sub(r"[ .'\-]", "", (text or "")).lower()

def default_date_et():
    """Return today's date string adjusted to Eastern Time."""
    eastern_now = datetime.now(ZoneInfo("America/New_York"))
    return eastern_now.strftime("%Y-%m-%d")

def load_starters(date_str: str, starters_dir: Path):
    """Load probable starters JSON from local file."""
    path = starters_dir / f"mlb_probable_starters_{date_str}.json"
    if not path.exists():
        print(f"⚠️ Probable starters file not found: {path}")
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"⚠️ Error loading starters: {e}")
        return []

def main():
    # default to repo-root/data/raw/weather and repo-root data for starters & CSV
    default_outdir    = Path.cwd() / "data" / "raw" / "weather"
    default_starters  = Path.cwd() / "data" / "raw" / "probable_starters"
    default_stadiums  = Path.cwd() / "mlb_stadium_coordinates.csv"

    parser = argparse.ArgumentParser(
        description="Fetch gametime weather for MLB probable starters and save locally."
    )
    parser.add_argument(
        "--date", type=str, default=default_date_et(),
        help="Date in YYYY-MM-DD format (default: today ET)"
    )
    parser.add_argument(
        "--outdir", type=Path, default=default_outdir,
        help="Output directory for weather JSON files"
    )
    parser.add_argument(
        "--starters-dir", type=Path, default=default_starters,
        help="Directory where probable starter JSON files are located"
    )
    parser.add_argument(
        "--stadium-csv", type=Path, default=default_stadiums,
        help="Path to the mlb_stadium_coordinates.csv file"
    )
    args = parser.parse_args()

    target_date = args.date
    outdir: Path = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)
    filename = f"mlb_weather_{target_date}.json"
    local_path = outdir / filename

    # Load starters
    starters = load_starters(target_date, args.starters_dir)
    if not starters:
        print("❌ No starters data; exiting.")
        return

    # Load stadium coordinates
    try:
        df = pd.read_csv(args.stadium_csv)
    except Exception as e:
        print(f"❌ Error reading stadium CSV: {e}")
        return
    df["TeamKey"] = df["Team"].apply(lambda x: normalize_key(x))
    stadium_map = {
        row.TeamKey: {
            "name": row.Stadium,
            "lat": row.Latitude,
            "lon": row.Longitude,
            "is_dome": bool(row.get("Is_Dome", False))
        }
        for _, row in df.iterrows()
    }
    # Override Oakland Athletics home
    override = {"name":"Sutter Health Park","lat":38.6254,"lon":-121.5050,"is_dome":False}
    for alias in ["oaklandathletics","athletics","sacramentoathletics","sutterhealthpark"]:
        stadium_map[alias] = override

    # Fetch weather
    records = []
    seen = set()
    for g in starters:
        # parse game datetime, drop tzinfo so we compare naive<->naive
        raw_dt = g.get("game_datetime")
        game_dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
        game_dt = game_dt.replace(tzinfo=None)

        for side in ("home_team", "away_team"):
            team = g.get(side)
            key = normalize_key(team)
            if key in seen:
                continue
            seen.add(key)
            info = stadium_map.get(key)
            if not info:
                print(f"⚠️ No stadium for team {team} (key: {key}); skipping")
                continue

            # Request weather
            params = {
                "latitude": info["lat"],
                "longitude": info["lon"],
                "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m,winddirection_10m,precipitation_probability,cloudcover,weathercode",
                "current_weather": True,
                "timezone": "auto"
            }
            try:
                resp = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"❌ Weather fetch failed for {team}: {e}")
                continue

            times = data.get("hourly", {}).get("time", [])
            idx = 0
            for i, t in enumerate(times):
                dt_t = datetime.fromisoformat(t)
                if dt_t >= game_dt:
                    idx = i
                    break

            temp_c = data["hourly"]["temperature_2m"][idx]
            temp_f = round(temp_c * 9/5 + 32, 1)
            wind_mph = round(data["hourly"]["windspeed_10m"][idx] * 0.621371, 1)
            # Normalize team name for Oakland variants
            if team in ["Athletics","A's","As","Sacramento Athletics","Sutter Health Park"]:
                team = "Oakland Athletics"

            records.append({
                "date": target_date,
                "team": team,
                "stadium": info["name"],
                "time_local": times[idx],
                "weather": {
                    "temperature_f": temp_f,
                    "humidity_pct": data["hourly"]["relativehumidity_2m"][idx],
                    "wind_speed_mph": wind_mph,
                    "wind_direction_deg": data["hourly"]["winddirection_10m"][idx],
                    "roof_status": "closed" if info["is_dome"] else "open"
                },
                "precipitation_probability": data["hourly"]["precipitation_probability"][idx],
                "cloud_cover_pct": data["hourly"]["cloudcover"][idx],
                "weather_code": data["hourly"]["weathercode"][idx]
            })
            time.sleep(1)

    # Save locally
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    print(f"💾 Saved weather to {local_path} ({len(records)} records)")

if __name__ == "__main__":
    main()
