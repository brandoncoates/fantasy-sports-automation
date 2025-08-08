#!/usr/bin/env python3
"""
Fetch MLB game-time weather for a given date’s probable starters and save locally.
"""
import argparse
import json
import time
import requests
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import pandas as pd

def normalize_key(text: str) -> str:
    return re.sub(r"[ .'\-]", "", (text or "")).lower()

def default_date_et():
    eastern_now = datetime.now(ZoneInfo("America/New_York"))
    return eastern_now.strftime("%Y-%m-%d")

def load_starters(date_str: str, starters_dir: Path):
    path = starters_dir / f"mlb_probable_starters_{date_str}.json"
    if not path.exists():
        print(f"⚠️ Probable starters file not found: {path}")
        return []
    return json.loads(path.read_text(encoding="utf-8"))

def main():
    repo_root     = Path(__file__).resolve().parent
    default_outdir= repo_root / "data" / "raw" / "weather"
    default_starters = repo_root / "data" / "raw" / "probable_starters"
    default_csv     = repo_root / "mlb_stadium_coordinates.csv"

    parser = argparse.ArgumentParser(
        description="Fetch gametime weather for MLB probable starters and save locally."
    )
    parser.add_argument("--date", type=str, default=default_date_et())
    parser.add_argument("--outdir", type=Path, default=default_outdir)
    parser.add_argument("--starters-dir", type=Path, default=default_starters)
    parser.add_argument("--stadium-csv", type=Path, default=default_csv)
    args = parser.parse_args()

    date   = args.date
    outdir = args.outdir; outdir.mkdir(parents=True, exist_ok=True)
    local_path = outdir / f"mlb_weather_{date}.json"

    starters = load_starters(date, args.starters_dir)
    if not starters:
        print("❌ No starters data; exiting.")
        return

    df = pd.read_csv(args.stadium_csv)
    df["TeamKey"] = df["Team"].apply(lambda x: normalize_key(x))
    stadium_map = {
        row.TeamKey: {
            "name":    row.Stadium,
            "lat":     row.Latitude,
            "lon":     row.Longitude,
            "is_dome": bool(row.get("Is_Dome", False))
        }
        for _, row in df.iterrows()
    }
    override = {"name":"Sutter Health Park","lat":38.6254,"lon":-121.5050,"is_dome":False}
    for alias in ("oaklandathletics","athletics","sacramentoathletics","sutterhealthpark"):
        stadium_map[alias] = override

    records = []; seen = set()
    for g in starters:
        game_dt = datetime.fromisoformat(g["game_datetime"].replace("Z","+00:00")).replace(tzinfo=None)
        for side in ("home_team","away_team"):
            team = g.get(side); key = normalize_key(team)
            if key in seen: continue
            seen.add(key)
            info = stadium_map.get(key)
            if not info:
                print(f"⚠️ No stadium for {team}; skipping")
                continue

            params = {
                "latitude": info["lat"],
                "longitude": info["lon"],
                "hourly":   ",".join([
                   "temperature_2m","relativehumidity_2m",
                   "windspeed_10m","winddirection_10m",
                   "precipitation_probability","cloudcover","weathercode"
                ]),
                "start_date": date,
                "end_date":   date,
                "timezone":   "auto"
            }
            try:
                resp = requests.get("https://api.open-meteo.com/v1/forecast",
                                    params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"❌ Weather fetch failed for {team}: {e}")
                continue

            times = data["hourly"]["time"]
            idx = next((i for i,t in enumerate(times)
                        if datetime.fromisoformat(t) >= game_dt), 0)

            temp_c = data["hourly"]["temperature_2m"][idx]
            temp_f = round(temp_c * 9/5 + 32, 1)
            wind_m = round(data["hourly"]["windspeed_10m"][idx] * 0.621371, 1)
            if team in ["Athletics","A's","As","Sacramento Athletics","Sutter Health Park"]:
                team = "Oakland Athletics"

            records.append({
                "date": date,
                "team": team,
                "stadium": info["name"],
                "time_local": times[idx],
                "weather": {
                    "temperature_f": temp_f,
                    "humidity_pct": data["hourly"]["relativehumidity_2m"][idx],
                    "wind_speed_mph": wind_m,
                    "wind_direction_deg": data["hourly"]["winddirection_10m"][idx],
                    "roof_status": "closed" if info["is_dome"] else "open"
                },
                "precipitation_probability": data["hourly"]["precipitation_probability"][idx],
                "cloud_cover_pct": data["hourly"]["cloudcover"][idx],
                "weather_code": data["hourly"]["weathercode"][idx]
            })
            time.sleep(1)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    print(f"💾 Saved weather to {local_path} ({len(records)} records)")

if __name__ == "__main__":
    main()
