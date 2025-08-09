#!/usr/bin/env python3
"""
streaks.py

Compute hot/cold streaks separately for batters and pitchers, based on a stat
column (default: 'metric' from compute_rolling_stats). Adds both role-specific
streak columns and unified legacy columns for downstream compatibility.

Role-specific outputs:
  - batter_current_streak_length, batter_streak_type, batter_streak_position
  - pitcher_current_streak_length, pitcher_streak_type, pitcher_streak_position

Legacy unified outputs (role-aware):
  - current_streak_length, streak_type, streak_position
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional, Iterable

import numpy as np
import pandas as pd


PITCHER_POSITIONS = {"P", "SP", "RP"}


def _to_num(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _streak_arrays(values: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Given a 1D numeric array, compute per-index:
      - current_streak_length: +N for hot (value>0), -N for cold (value<=0)
      - streak_type: 'hot'/'cold'
      - streak_position: abs(N)
    """
    n = len(values)
    length = np.zeros(n, dtype=int)
    stype = np.empty(n, dtype=object)
    spos  = np.zeros(n, dtype=int)

    cur = 0
    for i, v in enumerate(values):
        if v > 0:
            cur = cur + 1 if cur >= 0 else 1
            stype[i] = "hot"
        else:
            cur = cur - 1 if cur <= 0 else -1
            stype[i] = "cold"
        length[i] = cur
        spos[i] = abs(cur)

    return length, stype, spos


def annotate_streaks_separate(
    df: pd.DataFrame,
    batter_stat: str = "metric",
    pitcher_stat: str = "metric",
    date_column: str = "date",
) -> pd.DataFrame:
    """
    Compute streaks for batters and pitchers separately (based on `position`),
    then fill unified legacy columns role-aware.

    If `position` is missing, everyone is treated as batter for the role-specific
    outputs, and pitcher outputs are left NaN/0.

    Returns a new DataFrame with added columns.
    """
    if df is None or df.empty:
        out = df.copy() if df is not None else pd.DataFrame()
        for base in ["batter", "pitcher", ""]:
            prefix = (base + "_") if base else ""
            out[prefix + "current_streak_length"] = []
            out[prefix + "streak_type"] = []
            out[prefix + "streak_position"] = []
        return out

    work = df.copy()

    # Ensure date sortable
    if date_column in work.columns:
        work[date_column] = pd.to_datetime(work[date_column], errors="coerce")

    # Ensure stat columns exist and numeric
    if batter_stat not in work.columns:
        work[batter_stat] = 0.0
    if pitcher_stat not in work.columns:
        work[pitcher_stat] = 0.0

    work[batter_stat]  = work[batter_stat].apply(_to_num).fillna(0.0)
    work[pitcher_stat] = work[pitcher_stat].apply(_to_num).fillna(0.0)

    # If position missing, treat everyone as batter for role-separate outputs
    has_position = "position" in work.columns
    # Sort for chronological streaks
    sort_cols = ["player_id"]
    if date_column in work.columns:
        sort_cols.append(date_column)
    work = work.sort_values(sort_cols).reset_index(drop=True)

    # Prepare result arrays
    n = len(work)
    b_len = np.zeros(n, dtype=float) * np.nan
    b_typ = np.empty(n, dtype=object)
    b_pos = np.zeros(n, dtype=float) * np.nan

    p_len = np.zeros(n, dtype=float) * np.nan
    p_typ = np.empty(n, dtype=object)
    p_pos = np.zeros(n, dtype=float) * np.nan

    # Compute per player, role-aware
    for pid, grp in work.groupby("player_id", sort=False):
        idx = grp.index.to_numpy()

        # masks for batter vs pitcher for this group
        if has_position:
            pos_vals = grp["position"].astype(str).str.upper()
            is_pitcher = pos_vals.isin(PITCHER_POSITIONS).to_numpy()
        else:
            is_pitcher = np.zeros(len(idx), dtype=bool)  # no positions → all batter

        # --- batter rows
        b_idx = idx[~is_pitcher]
        if b_idx.size:
            vals = work.loc[b_idx, batter_stat].to_numpy(dtype=float)
            l, t, s = _streak_arrays(vals)
            b_len[b_idx] = l
            b_typ[b_idx] = t
            b_pos[b_idx] = s

        # --- pitcher rows
        p_idx = idx[is_pitcher]
        if p_idx.size:
            vals = work.loc[p_idx, pitcher_stat].to_numpy(dtype=float)
            l, t, s = _streak_arrays(vals)
            p_len[p_idx] = l
            p_typ[p_idx] = t
            p_pos[p_idx] = s

    # Attach role-specific columns
    work["batter_current_streak_length"] = b_len
    work["batter_streak_type"] = b_typ
    work["batter_streak_position"] = b_pos

    work["pitcher_current_streak_length"] = p_len
    work["pitcher_streak_type"] = p_typ
    work["pitcher_streak_position"] = p_pos

    # Fill unified legacy columns role-aware
    # If a row is pitcher, use pitcher_*; else use batter_* (or neutral defaults)
    if has_position:
        is_pitcher_all = work["position"].astype(str).str.upper().isin(PITCHER_POSITIONS)
    else:
        is_pitcher_all = pd.Series(False, index=work.index)

    def _pick(a, b):
        return np.where(is_pitcher_all.to_numpy(), a, b)

    # default neutral values where NaN
    def _fill(x, neutral_num=0, neutral_type="cold"):
        if x.dtype == object:
            return pd.Series(x).fillna(neutral_type).to_numpy()
        else:
            return pd.Series(x).fillna(neutral_num).to_numpy()

    work["current_streak_length"] = _pick(
        _fill(p_len, neutral_num=0), _fill(b_len, neutral_num=0)
    ).astype(int)
    work["streak_type"] = _pick(
        _fill(p_typ, neutral_type="cold"), _fill(b_typ, neutral_type="cold")
    )
    work["streak_position"] = _pick(
        _fill(p_pos, neutral_num=0), _fill(b_pos, neutral_num=0)
    ).astype(int)

    return work


# -------- Back-compat wrapper ----------------------------------------------

def annotate_streaks(
    df: pd.DataFrame,
    stat_column: str = "metric",
    date_column: str = "date",
) -> pd.DataFrame:
    """
    Backward-compatible wrapper that computes separate streaks but returns the
    role-aware unified columns too. Uses the same stat for batters and pitchers.
    """
    return annotate_streaks_separate(
        df, batter_stat=stat_column, pitcher_stat=stat_column, date_column=date_column
    )


# -------- Optional CLI for quick testing ------------------------------------

if __name__ == "__main__":
    import argparse
    from analyzer.data_loader import load_game_log, load_structured_players
    from analyzer.feature_engineering import compute_rolling_stats, merge_context

    p = argparse.ArgumentParser(description="Annotate batter/pitcher streaks")
    p.add_argument("--date", type=str, required=True)
    p.add_argument("--archive", type=Path, default=Path("player_game_log.jsonl"))
    p.add_argument("--structured", type=Path, default=None)
    p.add_argument("--bstat", type=str, default="metric", help="Batter stat (default: metric)")
    p.add_argument("--pstat", type=str, default="metric", help="Pitcher stat (default: metric)")
    args = p.parse_args()

    if args.structured is None:
        args.structured = Path(f"structured_players_{args.date}.json")

    logs = load_game_log(args.archive)
    struct = load_structured_players(args.structured)
    feats = compute_rolling_stats(logs)
    ctx = merge_context(feats, struct)

    out = annotate_streaks_separate(ctx, batter_stat=args.bstat, pitcher_stat=args.pstat)
    print(f"Annotated rows: {len(out)}")
