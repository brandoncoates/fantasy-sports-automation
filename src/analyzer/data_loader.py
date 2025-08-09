#!/usr/bin/env python3
# analyzer/data_loader.py
"""
Loaders for the analyzer:
- load_game_log: read JSONL history (one line per player-game)
- load_structured_players: read structured_players_<DATE>.json and KEEP nested dicts
"""

from pathlib import Path
from typing import Any, Dict, List
import json
import pandas as pd


def load_game_log(archive_path: Path) -> pd.DataFrame:
    """Read player_game_log.jsonl into a DataFrame (tolerant of missing/invalid lines)."""
    archive_path = Path(archive_path)
    records: List[Dict[str, Any]] = []
    if archive_path.exists():
        with archive_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    # skip malformed rows
                    continue
    df = pd.DataFrame(records)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def load_structured_players(structured_path: Path) -> pd.DataFrame:
    """
    Read structured_players_<DATE>.json and PRESERVE nested dicts like
    weather_context and betting_context. (Do NOT normalize/flatten here.)
    """
    p = Path(structured_path)
    if not p.exists():
        return pd.DataFrame()
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not data:
        return pd.DataFrame()
    df = pd.DataFrame.from_dict(data, orient="index").reset_index(drop=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


# Optional alias if other code calls load_structured(...)
def load_structured(structured_path: Path) -> pd.DataFrame:
    return load_structured_players(structured_path)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", type=Path, default=Path("player_game_log.jsonl"))
    ap.add_argument("--structured", type=Path, required=False)
    args = ap.parse_args()

    gl = load_game_log(args.archive)
    print(f"Game log rows: {len(gl)}")

    if args.structured:
        st = load_structured_players(args.structured)
        print(f"Structured rows: {len(st)}")
        print("Structured columns:", list(st.columns))
