#!/usr/bin/env python3
"""
ranking.py

Apply tier-based ranking to players based on weighted features, streak info,
and contextual fields (weather, betting, home/away).

Safe with:
- empty inputs
- missing columns
- nested dict fields (e.g., "weather_context.temperature_f")
"""

from __future__ import annotations
from pathlib import Path
from typing import Any, Mapping, Optional, Dict

import numpy as np
import pandas as pd

# Default weights (tune as you like)
FEATURE_WEIGHTS: Dict[str, float] = {
    "avg_last_3": 1.0,
    "avg_last_6": 1.0,
    "avg_last_10": 1.0,
    "current_streak_length": 0.5,
    # IMPORTANT: matches your combine output (weather_context is the dict)
    "weather_context.temperature_f": 0.2,
    "betting_context.over_under": 0.1,
}


# ----------------------------- helpers --------------------------------------

def _num(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _get_nested(obj: Any, dotted_key: str) -> Optional[float]:
    """
    Extract nested value like "weather_context.temperature_f" from a dict-typed cell.
    Returns numeric or None.
    """
    if not isinstance(obj, Mapping):
        return None
    cur = obj
    for part in dotted_key.split("."):
        if isinstance(cur, Mapping) and part in cur:
            cur = cur[part]
        else:
            return None
    return _num(cur)


# ----------------------------- core API -------------------------------------

def assign_tiers(df: pd.DataFrame, weights: Dict[str, float] = FEATURE_WEIGHTS) -> pd.DataFrame:
    """
    Compute a raw score for each row by combining features and weights, then map to a tier [0..10].
    Works even if df is empty or some features are missing.

    Returns a *new* DataFrame with 'raw_score' and 'tier' columns added.
    """
    if df is None or df.empty:
        out = df.copy() if df is not None else pd.DataFrame()
        out["raw_score"] = []
        out["tier"] = []
        return out

    df = df.copy()
    df["raw_score"] = 0.0

    for feature, w in weights.items():
        if "." in feature:
            # nested: "<column>.<subkey>[.<subsub>...]"
            col, subpath = feature.split(".", 1)
            if col in df.columns:
                df["raw_score"] += df[col].apply(lambda x: (_get_nested(x, subpath) or 0.0)) * float(w)
        else:
            # flat column
            if feature in df.columns:
                # try to coerce to numeric; non-numeric becomes NaN then filled 0
                vals = pd.to_numeric(df[feature], errors="coerce").fillna(0.0)
                df["raw_score"] += vals * float(w)

    # Map raw_score → tier 0..10 with min-max scaling
    if df["raw_score"].notna().any():
        rs = df["raw_score"].fillna(0.0)
        rmin, rmax = rs.min(), rs.max()
        if rmax > rmin:
            tiers = ((rs - rmin) / (rmax - rmin) * 10.0).round().astype(int)
        else:
            tiers = pd.Series(np.full(len(df), 5, dtype=int), index=df.index)  # neutral if no variance
        df["tier"] = tiers.clip(lower=0, upper=10)
    else:
        df["tier"] = 5  # default neutral

    return df


# ----------------------------- CLI (optional) -------------------------------

if __name__ == "__main__":
    import argparse
    from analyzer.data_loader import load_game_log, load_structured_players
    from analyzer.feature_engineering import compute_rolling_stats, merge_context
    from analyzer.streaks import annotate_streaks

    p = argparse.ArgumentParser(description="Assign tiers 0–10 to player-games")
    p.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    p.add_argument("--archive", type=Path, default=Path("player_game_log.jsonl"),
                   help="Path to game log (JSONL). Default: ./player_game_log.jsonl")
    p.add_argument("--structured", type=Path, default=None,
                   help="Path to structured JSON. Default: structured_players_<DATE>.json")
    p.add_argument("--output", type=Path, default=Path("data/analysis"),
                   help="Output directory for CSV (default: data/analysis)")
    args = p.parse_args()

    if args.structured is None:
        args.structured = Path(f"structured_players_{args.date}.json")
    args.output.mkdir(parents=True, exist_ok=True)

    print(f"📦 archive:    {args.archive.resolve()}")
    print(f"📦 structured: {args.structured.resolve()}")
    print(f"📂 output:     {args.output.resolve()}")

    logs = load_game_log(args.archive)
    structured = load_structured_players(args.structured)
    print(f"🔢 game log rows: {len(logs)}")
    print(f"🔢 structured rows: {len(structured)}")

    feats = compute_rolling_stats(logs)
    ctx = merge_context(feats, structured)
    streaked = annotate_streaks(ctx)

    ranked = assign_tiers(streaked, FEATURE_WEIGHTS)
    print(f"🏅 assigned tiers for {len(ranked)} rows")

    out_csv = args.output / f"tiers_{args.date}.csv"
    cols = ["player_id", "name", "date", "raw_score", "tier"]
    missing = [c for c in cols if c not in ranked.columns]
    if missing:
        # Be explicit if upstream changed
        raise KeyError(f"Missing expected columns in ranked output: {missing}")
    ranked.sort_values(["tier", "raw_score"], ascending=[False, False])[cols].to_csv(out_csv, index=False)
    print(f"💾 wrote tiers to {out_csv}")
