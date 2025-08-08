#!/usr/bin/env python3
"""
Fetch MLB game-time weather for a given date’s probable starters and save locally.
"""
import argparse, json, requests, re
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import pandas as pd

def normalize_key(text: str) -> str:
    return re.sub(r"[ .'\\-]", "", (text or "")).lower()

def default_date_et():
    eastern_now = datetime.now(ZoneInfo("America/New_York"))
    return eastern_now.strftime("%Y-%m-%d")

def load_starters(date_str: str, starters_dir: Path):
    path = starters_dir / f"mlb_probable_starters_{date_str}.json"
    if not path.exists():
        print(f"⚠️  No starters file: {path}")
        return []
    return json.loads(path.read_text(encoding="utf-8"))

def main():
    repo = Path.cwd()
    default_outdir   = repo/"data"/"raw"/"weather"
    default_starters = repo/"data"/"raw"/"probable_starters"
    default_csv      = repo/"mlb_stadium_coordinates.csv"

    p = argparse.ArgumentParser()
    p.add_argument("--date",       type=str,  default=default_date_et(),
                   help="YYYY-MM-DD (default yesterday ET)")
    p.add_argument("--outdir",     type=Path, default=default_outdir)
    p.add_argument("--starters-dir",type=Path, default=default_starters)
    p.add_argument("--stadium-csv", type=Path, default=default_csv)
    args = p.parse_args()

    date = args.date
    outdir = args.outdir; outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir/f"mlb_weather_{date}.json"

    starters = load_starters(date, args.starters_dir)
    if not starters:
        return

    # load stadiums
    df = pd.read_csv(args.stadium_csv)
    df["TeamKey"] = df["Team"].apply(lambda x: normalize_key(x))
    stadium_map = {
        row.TeamKey: {
            "name": row.Stadium,
            "lat":  row.Latitude,
            "lon":  row.Longitude,
            "is_dome": bool(row.get("Is_Dome", False))
        }
        for _, row in df.iterrows()
    }
    # override Oakland
    override = {"name":"Sutter Health Park","lat":38.6254,"lon":-121.5050,"is_dome":False}
    for a in ("oaklandathletics","athletics","sacramentoathletics","sutterhealthpark"):
        stadium_map[a] = override

    records = []
    seen = set()
    for g in starters:
        # parse the UTC game datetime, then drop tzinfo for naive compare
        raw_dt = g["game_datetime"].replace("Z","+00:00")
        game_dt = datetime.fromisoformat(raw_dt).replace(tzinfo=None)

        for side in ("home_team","away_team"):
            team = g[side]
            key  = normalize_key(team)
            if key in seen:
                continue
            seen.add(key)

            info = stadium_map.get(key)
            if not info:
                print(f"⚠️  No stadium for {team}; skipping")
                continue

            params = {
                "latitude": info["lat"],
                "longitude": info["lon"],
                "hourly": ",".join([
                   "temperature_2m",
                   "relativehumidity_2m",
                   "windspeed_10m",
                   "winddirection_10m",
                   "precipitation_probability",
                   "cloudcover",
                   "weathercode"
                ]),
                "start_date": date,
                "end_date":   date,
                "timezone":   "auto"
            }

            try:
                resp = requests.get(
                    "https://api.open-meteo.com/v1/forecast",
                    params=params,
                    timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"❌ Weather fetch failed for {team}: {e}")
                continue

            times = data["hourly"]["time"]
            # find the first index at or after game time
            idx = next(
                (i for i, t in enumerate(times)
                 if datetime.fromisoformat(t) >= game_dt),
                0
            )

            temp_c = data["hourly"]["temperature_2m"][idx]
            temp_f = round(temp_c * 9/5 + 32, 1)
            wind_m = round(data["hourly"]["windspeed_10m"][idx] * 0.621371, 1)

            # normalize Oakland variants
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
                "precipitation_probability":
                    data["hourly"]["precipitation_probability"][idx],
                "cloud_cover_pct": data["hourly"]["cloudcover"][idx],
                "weather_code": data["hourly"]["weathercode"][idx]
            })

    # write it out
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    print(f"💾 Saved {len(records)} weather records to {outpath}")

if __name__ == "__main__":
    main()
