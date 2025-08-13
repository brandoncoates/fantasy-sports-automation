# src/analyzer/conflicts.py
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Tuple, Literal
import pandas as pd

DropPolicy = Literal["prefer_sp", "prefer_hitters", "soft_penalty"]

@dataclass
class ConflictConfig:
    hitter_tier_min: float = 6.0
    sp_tier_min: float = 6.0
    drop_policy: DropPolicy = "prefer_sp"
    drop_sp_if_n_hit_gte: int = 2
    # soft penalty knobs
    conflict_tier_penalty: float = 0.4
    conflict_tier_penalty_max: float = 1.0

    @classmethod
    def from_env(cls) -> "ConflictConfig":
        def _f(key: str, default: float) -> float:
            v = os.getenv(key)
            return float(v) if v is not None and v != "" else default

        def _i(key: str, default: int) -> int:
            v = os.getenv(key)
            return int(v) if v is not None and v != "" else default

        drop_policy = os.getenv("DROP_POLICY", "prefer_sp").strip()
        if drop_policy not in ("prefer_sp", "prefer_hitters", "soft_penalty"):
            drop_policy = "prefer_sp"

        return cls(
            hitter_tier_min=_f("HITTER_TIER_MIN", 6.0),
            sp_tier_min=_f("SP_TIER_MIN", 6.0),
            drop_policy=drop_policy, 
            drop_sp_if_n_hit_gte=_i("DROP_SP_IF_N_HIT_GTE", 2),
            conflict_tier_penalty=_f("CONFLICT_TIER_PENALTY", 0.4),
            conflict_tier_penalty_max=_f("CONFLICT_TIER_PENALTY_MAX", 1.0),
        )

def _ensure_cols(df: pd.DataFrame, name: str) -> None:
    required = ["player_id","name","team","opponent_team","tier"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}. Got: {list(df.columns)}")

def detect_conflicts(hitters: pd.DataFrame, sps: pd.DataFrame, cfg: ConflictConfig) -> pd.DataFrame:
    """Return row-per-conflict between SP and opposing hitters."""
    _ensure_cols(hitters, "hitters")
    _ensure_cols(sps, "starting_pitchers")

    # Only consider actual SPs if that marker exists
    if "is_probable_starter" in sps.columns:
        sps = sps[sps["is_probable_starter"].fillna(True) != False].copy()

    conflicts = sps.merge(
        hitters,
        left_on=["team","opponent_team"],
        right_on=["opponent_team","team"],
        suffixes=("_sp","_h"),
        how="inner",
    )

    if "home_or_away_sp" in conflicts.columns and "home_or_away_h" in conflicts.columns:
        # Optional extra filter: ensure opposite sides of the same game
        conflicts = conflicts[conflicts["home_or_away_sp"] != conflicts["home_or_away_h"]]

    conflicts["hitter_ge_thresh"] = conflicts["tier_h"] >= cfg.hitter_tier_min
    conflicts["sp_ge_thresh"] = conflicts["tier_sp"] >= cfg.sp_tier_min

    # Reason is primarily informational for auditability
    conflicts["reason"] = (
        "opponent matchup; hitter>=min" + 
        f"({cfg.hitter_tier_min})"  # note: may be False in some rows; that's OK
    )
    keep_cols = [
        "player_id_sp","name_sp","team_sp","opponent_team_sp","tier_sp",
        "player_id_h","name_h","team_h","opponent_team_h","tier_h",
        "hitter_ge_thresh","sp_ge_thresh","reason"
    ]
    return conflicts[keep_cols].copy()

def resolve_conflicts(
    hitters: pd.DataFrame,
    sps: pd.DataFrame,
    cfg: ConflictConfig
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Apply the chosen policy and return (hitters_filtered, sps_filtered, conflicts_with_actions).
    For soft_penalty, no rows are dropped; instead 'tier' is reduced and
    columns 'tier_adjusted' are added to both frames.
    """
    _ensure_cols(hitters, "hitters")
    _ensure_cols(sps, "starting_pitchers")

    conflicts = detect_conflicts(hitters, sps, cfg)

    # fast path: nothing to do
    if conflicts.empty:
        conflicts["action"] = "none"
        if cfg.drop_policy == "soft_penalty":
            hitters["tier_adjusted"] = hitters["tier"]
            sps["tier_adjusted"] = sps["tier"]
        return hitters, sps, conflicts

    # Prepare decisions
    drop_hitters_ids = set()
    drop_sps_ids = set()

    if cfg.drop_policy in ("prefer_sp", "prefer_hitters"):
        # group by SP -> list of hitter rows
        grouped = conflicts.groupby(
            ["player_id_sp","name_sp","team_sp","opponent_team_sp","tier_sp"],
            as_index=False
        )
        actions = []

        for sp_key, g in grouped:
            sp_id, sp_name, sp_team, sp_opp, sp_tier = sp_key
            g_sorted = g.sort_values("tier_h", ascending=False)
            high_tier_count = (g_sorted["tier_h"] >= cfg.hitter_tier_min).sum()

            if cfg.drop_policy == "prefer_sp":
                # Drop hitters with strictly lower tier than this SP
                for _, row in g_sorted.iterrows():
                    if row["tier_h"] < sp_tier:
                        drop_hitters_ids.add(row["player_id_h"])
                        actions.append(("drop_hitter", sp_id, row["player_id_h"]))
                # If multiple legit high-tier hitters, drop the SP
                if high_tier_count >= cfg.drop_sp_if_n_hit_gte:
                    drop_sps_ids.add(sp_id)
                    actions.append(("drop_sp", sp_id, None))
            else:  # prefer_hitters
                # Drop SP if any hitter meets threshold
                if high_tier_count >= 1:
                    drop_sps_ids.add(sp_id)
                    actions.append(("drop_sp", sp_id, None))

        # Materialize filtered frames
        hitters_f = hitters[~hitters["player_id"].isin(drop_hitters_ids)].copy()
        sps_f = sps[~sps["player_id"].isin(drop_sps_ids)].copy()

        # annotate conflicts table with actions for transparency
        def _action_for_pair(r):
            if r["player_id_h"] in drop_hitters_ids and r["player_id_sp"] in drop_sps_ids:
                return "drop_both"
            if r["player_id_h"] in drop_hitters_ids:
                return "drop_hitter"
            if r["player_id_sp"] in drop_sps_ids:
                return "drop_sp"
            return "keep_both"

        conflicts["action"] = conflicts.apply(_action_for_pair, axis=1)
        return hitters_f, sps_f, conflicts

    # soft_penalty
    # Build penalties: A player can be involved in multiple conflicts, cap by max
    hitter_pen = conflicts.groupby("player_id_h")["player_id_sp"].count().rename("n_conflicts")
    sp_pen = conflicts.groupby("player_id_sp")["player_id_h"].count().rename("n_conflicts")

    hitters = hitters.copy()
    sps = sps.copy()
    hitters = hitters.merge(hitter_pen, left_on="player_id", right_index=True, how="left").fillna({"n_conflicts":0})
    sps = sps.merge(sp_pen, left_on="player_id", right_index=True, how="left").fillna({"n_conflicts":0})

    hitters["tier_adjusted"] = hitters["tier"] - (hitters["n_conflicts"] * cfg.conflict_tier_penalty)
    sps["tier_adjusted"] = sps["tier"] - (sps["n_conflicts"] * cfg.conflict_tier_penalty)
    hitters["tier_adjusted"] = hitters["tier_adjusted"].clip(lower=0, upper=10)
    sps["tier_adjusted"] = sps["tier_adjusted"].clip(lower=0, upper=10)

    # cap max total penalty
    max_drop = cfg.conflict_tier_penalty_max
    hitters["tier_adjusted"] = hitters[["tier","tier_adjusted"]].apply(
        lambda r: max(r["tier"], r["tier_adjusted"]) if r["tier"] - r["tier_adjusted"] > max_drop else r["tier_adjusted"],
        axis=1
    )
    sps["tier_adjusted"] = sps[["tier","tier_adjusted"]].apply(
        lambda r: max(r["tier"], r["tier_adjusted"]) if r["tier"] - r["tier_adjusted"] > max_drop else r["tier_adjusted"],
        axis=1
    )

    conflicts["action"] = "penalize_both"
    return hitters, sps, conflicts
