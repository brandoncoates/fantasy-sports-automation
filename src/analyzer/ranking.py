from __future__ import annotations

import numpy as np
import pandas as pd

# Lightweight, readable weights. Tune as needed.
HITTER_WEIGHTS = {
    # recent performance (if present)
    "avg_last_3": 0.80,
    "avg_last_6": 0.60,
    "avg_last_10": 0.45,
    "current_streak_length": 0.15,
    # context
    "ou_total": 0.50,          # higher total -> better for hitters
    "temp_f": 0.10,            # warmer tends to help offense
    "wind_mph": 0.05,          # generic small bump; direction not modeled here
    "home_bonus": 0.15,        # home/away
    "is_switch_hitter": 0.05,  # small bonus
}

SP_WEIGHTS = {
    # context (we avoid hitter avg_* which are at-bat metrics)
    "ou_total": -0.80,         # LOWER total -> better for pitchers
    "is_favorite": 0.30,       # favored teams correlate with SP win/defense
    "temp_f": -0.10,           # heat often boosts bats
    "wind_mph": -0.05,         # wind can aid offense
    "precip_prob": 0.05,       # light positive: wet/cool may dampen offense; games can postpone though
    # make sure they're actually starting today
    "starting_pitcher_today": 1.0,
}


def _col(df: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    s = df[name] if name in df.columns else pd.Series(default, index=df.index)
    return s.fillna(default)


def _compute_linear(df: pd.DataFrame, weights: dict) -> pd.Series:
    total = pd.Series(0.0, index=df.index)
    for k, w in weights.items():
        total = total + _col(df, k, 0.0) * float(w)
    return total


def _percentile_to_tier(scores: pd.Series) -> pd.Series:
    if scores.empty:
        return scores
    # Stable percentile rank 0..1; map to 0..10 (int)
    # If all equal, return 5s.
    if scores.nunique(dropna=False) <= 1:
        return pd.Series(5, index=scores.index, dtype=int)
    pct = scores.rank(pct=True, method="average").clip(0, 1)
    return (pct * 10).round().astype(int).clip(0, 10)


def assign_tiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute raw_score and tier using separate formulas for hitters vs starting pitchers.
    Tiers are mapped within-group to avoid cross-position dilution.
    """
    if df.empty:
        return df.assign(raw_score=0.0, tier=0)

    out = df.copy()

    # home/away indicator for hitters
    hoa = out.get("home_or_away")
    out["home_bonus"] = ((hoa.astype(str).str.lower() == "home") if hoa is not None else 0).astype(int)

    # Group masks
    pos = out.get("position", pd.Series(index=out.index, dtype="object"))
    is_pitcher_position = pos.astype(str).isin(["P", "SP", "RP"])
    is_sp_today = out.get("starting_pitcher_today", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    sp_mask = (is_pitcher_position & is_sp_today)
    hitter_mask = (~is_pitcher_position)

    # Compute group scores
    hitter_scores = _compute_linear(out[hitter_mask], HITTER_WEIGHTS) if hitter_mask.any() else pd.Series(dtype=float)
    sp_scores = _compute_linear(out[sp_mask], SP_WEIGHTS) if sp_mask.any() else pd.Series(dtype=float)

    # Merge back into full index
    raw = pd.Series(0.0, index=out.index)
    raw.loc[hitter_mask] = hitter_scores
    raw.loc[sp_mask] = sp_scores

    # Tier mapping within each group
    tiers = pd.Series(0, index=out.index, dtype=int)
    if hitter_mask.any():
        tiers.loc[hitter_mask] = _percentile_to_tier(raw.loc[hitter_mask])
    if sp_mask.any():
        tiers.loc[sp_mask] = _percentile_to_tier(raw.loc[sp_mask])

    out["raw_score"] = raw
    out["tier"] = tiers

    # Clip to 0..10 and ensure numeric
    out["raw_score"] = out["raw_score"].astype(float)
    out["tier"] = out["tier"].astype(int).clip(0, 10)

    return out
