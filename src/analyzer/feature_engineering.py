#!/usr/bin/env python3
# analyzer/feature_engineering.py
"""
Feature engineering for MLB pipeline.

Key change: we standardize on the *actual game date* for all joins.
- If a DataFrame has 'game_date', we use it.
- Otherwise, we fall back to 'date'.
- Outputs continue to expose a 'date' column so downstream code (ranking/evaluation)
  keeps working without changes.

Public API:
- compute_rolling_stats(log_df)
- merge_context(feat_df, structured_df)
"""

from __future__ import annotations
from typing import Dict, Any
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
    - For batters: approximate total bases (singles + 2*2B + 3*3B + 4*HR) + small BB credit
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


def _resolve_game_date_column(df: pd.DataFrame, *, box_col: str | None = None) -> pd.Series:
    """
    Return a pandas Series representing the actual game date for each row:
      - Prefer explicit 'game_date' column if present.
      - Else use 'date'.
      - If neither exists but a 'box_score' dict is present, attempt box_score['game_date'].
    The result is normalized to pandas datetime64[ns] (date component only).
    """
    s = None

    if "game_date" in df.columns:
        s = df["game_date"]
    elif "date" in df.columns:
        s = df["date"]
    elif box_col and box_col in df.columns:
        # Attempt to pull from nested box_score dicts
        s = df[box_col].apply(lambda b: (b or {}).get("game_date") if isinstance(b, dict) else None)
    else:
        s = pd.Series([None] * len(df), index=df.index)

    s = pd.to_datetime(s, errors="coerce").dt.date
    return s


# ------- Public API ----------------------------------------------------------

def compute_rolling_stats(log_df: pd.DataFrame) -> pd.DataFrame:
    """
    Given the JSONL game log (one row per player-game), compute rolling features
    keyed by the actual *game date*.

    Returns a DataFrame with at least:
      ['player_id','name','date','metric','avg_last_3','avg_last_6','avg_last_10']

    If log_df is empty or missing key columns, returns an empty DataFrame with
    those columns (so downstream code won't KeyError).
    """
    # Expected minimum inputs
    needed = {"player_id", "name", "box_score"}
    if log_df is None or log_df.empty or not needed.issubset(set(log_df.columns)):
        cols = ["player_id", "name", "date", "metric", "avg_last_3", "avg_last_6", "avg_last_10"]
        return pd.DataFrame(columns=cols)

    df = log_df[["player_id", "name", "box_score"]].copy()

    # Resolve actual game date
    df["date"] = _resolve_game_date_column(log_df, box_col="box_score")
    df = df[df["date"].notna()]  # drop rows without a usable date
    if df.empty:
        cols = ["player_id", "name", "date", "metric", "avg_last_3", "avg_last_6", "avg_last_10"]
        return pd.DataFrame(columns=cols)

    # Compute per-game metric
    df["metric"] = df["box_score"].apply(_metric_from_box).astype(float)
    df["metric"] = df["metric"].fillna(0.0)

    # Sort & group by actual game date
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
    using the actual game date.

    Keys:
      - feature side: 'player_id' + resolved game date (from feat_df['date'])
      - structured side: prefer 'game_date', else 'date'
    We preserve downstream compatibility by keeping 'date' in the output.
    """
    if feat_df is None or feat_df.empty:
        return pd.DataFrame(columns=[
            "player_id", "name", "date", "metric", "avg_last_3", "avg_last_6", "avg_last_10"
        ])

    # Ensure 'date' in feat_df is datetime.date (game date already from compute_rolling_stats)
    feat = feat_df.copy()
    feat["date"] = pd.to_datetime(feat["date"], errors="coerce").dt.date

    # Structured may have both 'date' and 'game_date' (we prefer 'game_date')
    st = structured_df.copy() if structured_df is not None else pd.DataFrame()
    if not st.empty:
        # Create a unified game-date column on structured side
        st["_merge_date"] = _resolve_game_date_column(st)
        # Normalize date types
        st["_merge_date"] = pd.to_datetime(st["_merge_date"], errors="coerce").dt.date

    # Columns we want from structured; keep only those that exist (exclude duplicate 'player_id')
    wanted = [
        'player_id',
        'name',
        'team',
        'opponent_team',
        'home_or_away',
        'position',
        'is_probable_starter',
        'starter',
        'weather_context',
        'betting_context',
    ]
    keep_no_id = [c for c in wanted if c in st.columns and c != "player_id"] if not st.empty else []
    ctx = pd.DataFrame()
    if keep_no_id:
        ctx = st[["player_id", "_merge_date", *keep_no_id]].copy()
        # Drop duplicates by player/date to avoid exploding joins
        ctx = ctx.drop_duplicates(subset=["player_id", "_merge_date"])

    # Left join: keep all rows from feat_df on actual game date
    if not ctx.empty:
        out = pd.merge(
            feat,
            ctx.rename(columns={"_merge_date": "date"}),  # align key name for merge
            on=["player_id", "date"],
            how="left",
            suffixes=("", "_ctx"),
        )
    else:
        out = feat.copy()

    return out


# ---------------- CLI (optional quick test) ----------------

if __name__ == "__main__":
    import argparse
    from analyzer.data_loader import load_game_log, load_structured_players

    p = argparse.ArgumentParser(description="Feature engineering (game-date aware)")
    p.add_argument("--archive", type=Path, default=Path("player_game_log.jsonl"))
    p.add_argument("--structured", type=Path, required=False)
    args = p.parse_args()

    gl = load_game_log(args.archive)
    print(f"Game log rows: {len(gl)}")

    st = pd.DataFrame()
    if args.structured:
        st = load_structured_players(Path(args.structured))
        print(f"Structured rows: {len(st)}")
        print("Structured columns:", list(st.columns))

    feats = compute_rolling_stats(gl)
    print("Feature rows:", len(feats))
    print("Feature columns:", list(feats.columns))

    merged = merge_context(feats, st)
    print("Merged rows:", len(merged))
    print("Merged columns:", list(merged.columns))
