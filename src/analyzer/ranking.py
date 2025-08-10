#!/usr/bin/env python3
"""
ranking.py

Assign tier-based scores to player rows by combining recent-performance features
and contextual features (weather, betting, home/away). Safe with empty inputs
and missing columns. Supports nested dict fields via dotted keys
(e.g., "weather_context.temperature_f").

You can override weights at runtime:
  1) Set env var RANKING_WEIGHTS_JSON to a JSON object string
     e.g. '{"avg_last_3":1.2,"betting_context.over_under":0.25}'
  2) Or set env var RANKING_WEIGHTS_PATH to a JSON file path
  3) Or place a local "ranking_weights.json" next to this file

Precedence: env JSON > env PATH file > local file > defaults.
"""

from __future__ import annotations

import json
import os
from typing import Any, Mapping, Optional, Dict

import numpy as np
import pandas as pd


# ----------------------------- default weights ------------------------------

DEFAULT_FEATURE_WEIGHTS: Dict[str, float] = {
    "avg_last_3": 1.0,
    "avg_last_6": 1.0,
    "avg_last_10": 1.0,
    "current_streak_length": 0.5,
    # Matches your combine output: weather_context and betting_context are dicts
    "weather_context.temperature_f": 0.2,
    "betting_context.over_under": 0.1,
}


def _load_weights_from_env_json() -> Optional[Dict[str, float]]:
    raw = os.getenv("RANKING_WEIGHTS_JSON")
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return {str(k): float(v) for k, v in obj.items()}
    except Exception:
        pass
    return None


def _load_weights_from_env_path() -> Optional[Dict[str, float]]:
    path = os.getenv("RANKING_WEIGHTS_PATH")
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return {str(k): float(v) for k, v in obj.items()}
    except Exception:
        pass
    return None


def _load_weights_from_local_file() -> Optional[Dict[str, float]]:
    here = os.path.dirname(os.path.abspath(__file__))
    candidate = os.path.join(here, "ranking_weights.json")
    if not os.path.exists(candidate):
        return None
    try:
        with open(candidate, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return {str(k): float(v) for k, v in obj.items()}
    except Exception:
        pass
    return None


def load_feature_weights() -> Dict[str, float]:
    """
    Load weights with precedence:
      env JSON > env PATH file > local file > defaults.
    """
    for loader in (_load_weights_from_env_json, _load_weights_from_env_path, _load_weights_from_local_file):
        w = loader()
        if w:
            # Merge onto defaults so missing keys still exist
            merged = DEFAULT_FEATURE_WEIGHTS.copy()
            merged.update(w)
            return merged
    return DEFAULT_FEATURE_WEIGHTS.copy()


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

def assign_tiers(df: pd.DataFrame, weights: Optional[Dict[str, float]] = None) -> pd.DataFrame:
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

    if weights is None:
        weights = load_feature_weights()

    df = df.copy()
    df["raw_score"] = 0.0

    for feature, w in weights.items():
        if "." in feature:
            # nested: "<column>.<subkey>[.<subsub>...]"
            col, subpath = feature.split(".", 1)
            if col in df.columns:
                df["raw_score"] += df[col].apply(lambda x: (_get_nested(x, subpath) or 0.0)) * float(w)
        else:
            if feature in df.columns:
                vals = pd.to_numeric(df[feature], errors="coerce").fillna(0.0)
                df["raw_score"] += vals * float(w)

    # Map raw_score → tier 0..10 with min-max scaling
    rs = pd.to_numeric(df["raw_score"], errors="coerce").fillna(0.0)
    rmin, rmax = rs.min(), rs.max()
    if rmax > rmin:
        tiers = ((rs - rmin) / (rmax - rmin) * 10.0).round().astype(int)
    else:
        tiers = pd.Series(np.full(len(df), 5, dtype=int), index=df.index)  # neutral if no variance
    df["tier"] = tiers.clip(lower=0, upper=10)

    return df


# ----------------------------- optional CLI ---------------------------------

if __name__ == "__main__":
    import argparse
    from pathlib import Path
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
    p.add_argument("--weights-json", type=str, default=None,
                   help="Optional JSON string of weights to override defaults.")
    args = p.parse_args()

    if args.structured is None:
        args.structured = Path(f"structured_players_{args.date}.json")
    args.output.mkdir(parents=True, exist_ok=True)

    logs = load_game_log(args.archive)
    structured = load_structured_players(args.structured)
    feats = compute_rolling_stats(logs)
    ctx = merge_context(feats, structured)
    streaked = annotate_streaks(ctx)

    custom_weights = None
    if args.weights_json:
        try:
            w = json.loads(args.weights_json)
            if isinstance(w, dict):
                custom_weights = DEFAULT_FEATURE_WEIGHTS.copy()
                custom_weights.update({str(k): float(v) for k, v in w.items()})
        except Exception:
            pass

    ranked = assign_tiers(streaked, custom_weights)
    cols = ["player_id", "name", "date", "raw_score", "tier"]
    out_csv = args.output / f"tiers_{args.date}.csv"
    ranked.sort_values(["tier", "raw_score"], ascending=[False, False])[cols].to_csv(out_csv, index=False)
    print(f"💾 wrote tiers to {out_csv}")
