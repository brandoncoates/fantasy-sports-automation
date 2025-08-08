#!/usr/bin/env python3
"""
mlb_combine_all_files.py

Combine raw JSON into structured_players_{DATE}.json and append to player_game_log.jsonl.
"""
import argparse, json, re, glob
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

def normalize(text: str) -> str:
    return re.sub(r"[ .'\\-]", "", (text or "")).lower()

def load_json(path: Path):
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date","-d",
                   default=datetime.utcnow().strftime("%Y-%m-%d"),
                   help="YYYY-MM-DD")
    p.add_argument("--raw-dir", type=Path, default=Path("data")/"raw")
    args = p.parse_args()

    date = args.date
    yday = (datetime.strptime(date,"%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    raw = args.raw_dir

    # Load sources
    rosters   = load_json(raw/"rosters"/f"mlb_rosters_{date}.json")
    starters  = load_json(raw/"probable_starters"/f"mlb_probable_starters_{date}.json")
    weather   = load_json(raw/"weather"/f"mlb_weather_{date}.json")
    odds      = load_json(raw/"betting"/f"mlb_betting_odds_{date}.json")
    boxscores = load_json(raw/"boxscores"/f"mlb_boxscores_{yday}.json")

    # Build team maps
    team_map = { normalize(g[k]): g[k] 
                 for g in starters for k in ("home_team","away_team") }

    # Earliest weather per team
    weather_by = {}
    for w in weather:
        team = team_map.get(normalize(w["team"]), w["team"])
        if team not in weather_by or w["time_local"] < weather_by[team]["time_local"]:
            weather_by[team] = w

    # Betting & matchups
    bet, matchup = {}, {}
    for o in odds:
        if o.get("bookmaker")!="FanDuel": continue
        h,a = o["home_team"], o["away_team"]
        info = {
          "over_under": o.get("over_under"),
          "spread": o.get("spread"),
          "favorite": o.get("favorite"),
          "underdog": o.get("underdog"),
          "implied_totals": o.get("implied_totals", {}),
        }
        bet[h]=bet[a]=info
        matchup[normalize(h)]={"opponent":a,"home_or_away":"home"}
        matchup[normalize(a)]={"opponent":h,"home_or_away":"away"}

    # Ensure every starter has a matchup
    for g in starters:
        for side in ("home_team","away_team"):
            t = g[side]; n=normalize(t)
            if n not in matchup:
                opp = g["away_team"] if side=="home_team" else g["home_team"]
                matchup[n]={"opponent":opp,"home_or_away":("home" if side=="home_team" else "away")}

    # Box by player
    box_by = { normalize(b["player_name"]):b for b in boxscores }

    # Prepare outputs
    archive = Path("player_game_log.jsonl")
    out_struct = Path(f"structured_players_{date}.json")
    players = {}

    with archive.open("a",encoding="utf-8") as arch:
        for r in rosters:
            pid = str(r["player_id"])
            name = r["player"]
            team = r["team"]
            canon = team_map.get(normalize(team), team)
            m = matchup.get(normalize(canon),{})
            w = weather_by.get(canon,{})
            bd = bet.get(canon,{})

            # box score
            box = box_by.get(normalize(name),{}).copy()
            if r.get("position") not in ["P","SP","RP"]:
                for stat in ["innings_pitched","earned_runs","strikeouts_pitch","wins","quality_start"]:
                    box.pop(stat,None)

            # probable starter?
            snames = { normalize(g["home_pitcher"]) for g in starters } \
                     | { normalize(g["away_pitcher"]) for g in starters }
            is_st = normalize(name) in snames

            # structured entry
            players[name] = {
              "player_id":pid,"name":name,"team":canon,
              "opponent_team":m.get("opponent"),"home_or_away":m.get("home_or_away"),
              "position":r.get("position"),"handedness":{"bats":r.get("bats"),"throws":r.get("throws")},
              "roster_status":{"status_code":r.get("status_code"),"status_description":r.get("status_description")},
              "is_probable_starter":is_st,"starter":is_st,
              "weather_context":w.get("weather",{}),"betting_context":bd,
              "espn_mentions":0,"espn_articles":[],"reddit_mentions":0,
              "box_score":box,
            }

            # append archive
            if box:
                entry = {
                  "date": yday,"player_id":pid,"name":name,
                  "team":canon,"opponent":m.get("opponent"),
                  "home_or_away":m.get("home_or_away"),
                  "box_score":box,"weather":w.get("weather",{}),"betting":bd
                }
                arch.write(json.dumps(entry)+"\n")

    # write structured JSON
    out_struct.write_text(json.dumps(players,indent=2),encoding="utf-8")
    print(f"✅ Wrote {len(players)} players to {out_struct}")

if __name__=="__main__":
    main()
