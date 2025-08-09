#!/usr/bin/env python3
# analyzer/feature_engineering.py
"""
Feature engineering for MLB pipeline.

- compute_rolling_stats(log_df):
    Builds per-player rolling features from player_game_log.jsonl.
    Works even if the log is empty (returns an empty DF with expected columns).

- merge_context(feat_df, structured_df):
    Merges rolling features with per-player context (weather/betting/home/away/opponent).
    If feat_df is empty (no history yet), it seeds rows from structured_df so
    downstream steps still have data for the pipeline date.
"""

from __future__ import annotations
from typing import Dict, Any, Iterable
from pathlib import Path

import numpy as np
import pandas as pd


# ------- Helpers -------------------------------------------------------------

def _to_num(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


def _metric_from_box(box: Dict[str, Any]) -> float | None:
    """
    Build a simple per-game performance metric from a box_score dict.
    - For batters: approximate total bases (singles+2*2B+3*3B+4*HR)
    - For pitchers: strikeouts - earned runs (very simple proxy)
    Returns None if no useful data.
    """
    if not isinstance(box, dict) or not box:
        return None

    # Batter path (prefer if we see batting stats)
    hits = _to_num(box.get("hits"))
    ab   = _to_num(box.get("at_bats"))
    hr   = _to_num(box.get("home_runs"))
    dbl  = _to_num(box.get("doubles"))
    trp  = _to_num(box.get("triples"))
    bb   = _to_num(box.get("walks"))

    # If it looks like a batter line, compute total bases proxy
    if any(v is not None for v in [hits, ab, hr, dbl, trp, bb]):
        hits = hits or 0.0
        hr   = hr or 0.0
        dbl  = dbl or 0.0
        trp  = trp or 0.0
        singles = max(hits - (hr + dbl + trp), 0.0)
        total_bases = singles + 2*dbl + 3*trp + 4*hr
        # Small credit for walks
        total_bases += (bb or 0.0) * 0.2
        return float(total_bases)

    # Pitcher path
    k  = _to_num(box.get("strikeouts_pitch"))
    er = _to_num(box.get("earned_runs"))
    if k is not None or er is not None:
        k  = k or 0.0
        er = er or 0.0
        return float(k - er)

    return None


def _ensure_datetime(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")


# ------- Public API ----------------------------------------------------------

def compute_rolling_stats(log_df: pd.DataFrame) -> pd.DataFrame:
    """
    Given the JSONL game log (one row per player-game), compute rolling features.

    Returns a DataFrame with at least:
      ['player_id','name','date','metric','avg_last_3','avg_last_6','avg_last_10']

    If log_df is empty or missing key columns, returns an empty DataFrame with
    those columns (so downstream code won't KeyError).
    """
    # Expected minimum inputs
    needed = {"player_id", "name", "date", "box_score"}
    if log_df is None or log_df.empty or not needed.issubset(set(log_df.columns)):
        cols = ["player_id", "name", "date", "metric", "avg_last_3", "avg_last_6", "avg_last_10"]
        return pd.DataFrame(columns=cols)

    df = log_df[["player_id", "name", "date", "box_score"]].copy()
    _ensure_datetime(df, "date")
    df = df[df["date"].notna()]
    if df.empty:
        cols = ["player_id", "name", "date", "metric", "avg_last_3", "avg_last_6", "avg_last_10"]
        return pd.DataFrame(columns=cols)

    # Compute per-game metric
    df["metric"] = df["box_score"].apply(_metric_from_box).astype(float)
    df["metric"] = df["metric"].fillna(0.0)

    # Sort & group
    df = df.sort_values(["player_id", "date"])
    g = df.groupby("player_id", group_keys=False)

    # Rolling windows (include current game)
    df["avg_last_3"]  = g["metric"].rolling(window=3,  min_periods=1).mean().reset_index(level=0, drop=True)
    df["avg_last_6"]  = g["metric"].rolling(window=6,  min_periods=1).mean().reset_index(level=0, drop=True)
    df["avg_last_10"] = g["metric"].rolling(window=10, min_periods=1).mean().reset_index(level=0, drop=True)

    # Select minimal set used downstream
    out_cols = ["player_id", "name", "date", "metric", "avg_last_3", "avg_last_6", "avg_last_10"]
    return df[out_cols].copy()


def merge_context(feat_df: pd.DataFrame, structured_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge rolling features (feat_df) with structured context (structured_df).
    - Ensures required context columns exist.
    - Ensures date alignment.
    - If feat_df is empty (e.g., no history yet), seeds rows from structured_df
      for the pipeline date so downstream steps still have data.
    """
    # Defensive copies
    s = structured_df.copy() if structured_df is not None else pd.DataFrame()
    f = feat_df.copy() if feat_df is not None else pd.DataFrame()

    # Required context columns for downstream
    required_ctx = ["player_id", "date", "weather_context", "betting_context", "home_or_away", "opponent_team", "name"]
    for col in required_ctx:
        if col not in s.columns:
            s[col] = None

    _ensure_datetime(s, "date")
    _ensure_datetime(f, "date")

    # If no features (fresh start), seed rows from structured for that date
    if f.empty:
        # Seed with player_id, name, date from structured
        seed = s[["player_id", "name", "date"]].copy()
        # Provide neutral feature defaults
        for col in ["metric", "avg_last_3", "avg_last_6", "avg_last_10"]:
            seed[col] = 0.0
        f = seed

    # Prepare context slice for merge
    ctx = s[["player_id", "date", "weather_context", "betting_context", "home_or_away", "opponent_team"]].copy()

    # Merge features with context
    out = f.merge(ctx, on=["player_id", "date"], how="left")

    return out
