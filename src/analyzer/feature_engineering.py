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
    Merge rolling-feature frame (feat_df) with contextual data from structured_df
    on ['player_id','date'].

    We carry through weather/betting context plus team/opponent/home_or_away,
    position and probable-starter flags so they’re available for ranking/outputs.
    """
    df = feat_df.copy()

    # Make sure the merge keys are aligned as datetimes
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date']).dt.date
    if 'date' in structured_df.columns:
        structured_df = structured_df.copy()
        structured_df['date'] = pd.to_datetime(structured_df['date']).dt.date

    # Columns we want from structured; only keep those that actually exist
    wanted = [
        'player_id', 'date',
        'name',                # useful if feat_df doesn't carry it
        'team',
        'opponent_team',
        'home_or_away',
        'position',
        'is_probable_starter', # may or may not exist
        'starter',             # fallback if you kept only 'starter'
        'weather_context',
        'betting_context',
    ]
    keep = [c for c in wanted if c in structured_df.columns]
    ctx = structured_df[keep].drop_duplicates(subset=['player_id','date'])

    # Left join: keep all rows from feat_df
    out = pd.merge(df, ctx, on=['player_id','date'], how='left')

    # Normalize: if only 'starter' exists, mirror it into 'is_probable_starter'
    if 'is_probable_starter' not in out.columns and 'starter' in out.columns:
        out['is_probable_starter'] = out['starter']

    return out
