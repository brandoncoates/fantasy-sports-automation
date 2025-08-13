from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Tuple, Literal
import pandas as pd

DropPolicy = Literal["prefer_sp", "prefer_hitters", "soft_penalty"]


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v in (None, ""):
        return default
    try:
        return float(v)
    except ValueError:
        return default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v in (None, ""):
        return default
    try:
        return int(v)
    except ValueError:
        return default


@dataclass
class ConflictConfig:
    hitter_tier_min: float = 6.0
    sp_tier_min: float = 6.0
    drop_policy: DropPolicy = "prefer_sp"
    drop_sp_if_n_hit_gte: int = 2
    # soft penalty knobs
    conflict_tier_penalty: float = 0.4
    conflict_tier_penalty_max: float = 1.0
    # NEW: cap hitters vs top-tier SPs
    top_sp_tier_min: float = 9.0
    max_hitters_vs_top_sp: int = 2
    hitter_over_sp_margin: float = 0.0
    min_keep_vs_top_sp: int = 1

    @classmethod
    def from_env(cls) -> "ConflictConfig":
        return cls(
            hitter_tier_min=_env_float("HITTER_TIER_MIN", 6.0),
            sp_tier_min=_env_float("SP_TIER_MIN", 6.0),
            drop_policy=_env_str("DROP_POLICY", "prefer_sp"),
            drop_sp_if_n_hit_gte=_env_int("DROP_SP_IF_N_HIT_GTE", 2),
            conflict_tier_penalty=_env_float("CONFLICT_TIER_PENALTY", 0.4),
            conflict_tier_penalty_max=_env_float("CONFLICT_TIER_PENALTY_MAX", 1.0),
            top_sp_tier_min=_env_float("TOP_SP_TIER_MIN", 9.0),
            max_hitters_vs_top_sp=_env_int("MAX_HITTERS_VS_TOP_SP", 2),
            hitter_over_sp_margin=_env_float("HITTER_OVER_SP_MARGIN", 0.0),
            min_keep_vs_top_sp=_env_int("MIN_KEEP_VS_TOP_SP", 1),
        )


def _ensure_cols(df: pd.DataFrame, name: str) -> None:
    required = ["player_id", "name", "team", "opponent_team", "tier"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}. Got: {list(df.columns)}")


def detect_conflicts(hitters: pd.DataFrame, sps: pd.DataFrame, cfg: ConflictConfig) -> pd.DataFrame:
    """
    Return row-per-conflict between an SP and opposing hitters (team/opponent mirror match).
    Adds boolean flags for threshold checks and a human-readable reason.
    """
    _ensure_cols(hitters, "hitters")
    _ensure_cols(sps, "starting_pitchers")

    # Only keep probable starters if that marker exists
    if "is_probable_starter" in sps.columns:
        sps = sps[sps["is_probable_starter"].fillna(True) != False].copy()

    # Inner-join on mirrored team/opponent
    conflicts = sps.merge(
        hitters,
        left_on=["team", "opponent_team"],
        right_on=["opponent_team", "team"],
        suffixes=("_sp", "_h"),
        how="inner",
    )

    # Optional sanity: if both have home_or_away, ensure they are opposite sides
    if "home_or_away_sp" in conflicts.columns and "home_or_away_h" in conflicts.columns:
        conflicts = conflicts[conflicts["home_or_away_sp"] != conflicts["home_or_away_h"]]

    # Numeric safety
    for col in ("tier_sp", "tier_h"):
        if col in conflicts.columns:
            conflicts[col] = pd.to_numeric(conflicts[col], errors="coerce")

    conflicts["hitter_ge_thresh"] = conflicts["tier_h"] >= cfg.hitter_tier_min
    conflicts["sp_ge_thresh"] = conflicts["tier_sp"] >= cfg.sp_tier_min
    conflicts["reason"] = f"opponent matchup; hitter>=min({cfg.hitter_tier_min})"

    keep_cols = [
        "player_id_sp", "name_sp", "team_sp", "opponent_team_sp", "tier_sp",
        "player_id_h", "name_h", "team_h", "opponent_team_h", "tier_h",
        "hitter_ge_thresh", "sp_ge_thresh", "reason",
    ]
    return conflicts[keep_cols].copy()


def resolve_conflicts(
    hitters: pd.DataFrame,
    sps: pd.DataFrame,
    cfg: ConflictConfig
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Apply the chosen policy and return (hitters_filtered, sps_filtered, conflicts_with_actions).

    Policies:
      - prefer_sp: drop hitters with tier < SP tier for that matchup; if >=N hitters meet
        hitter_tier_min against the same SP, drop that SP.
        NEW: if SP tier >= top_sp_tier_min, keep at most max_hitters_vs_top_sp opposing hitters;
             optionally require hitter >= SP tier + hitter_over_sp_margin. Others are dropped.
      - prefer_hitters: drop SP if any opposing hitter meets hitter_tier_min; keep hitters.
      - soft_penalty: keep all rows; subtract conflict_tier_penalty per conflict from both
        sides (capped by conflict_tier_penalty_max). Adds 'tier_adjusted' cols.
    """
    _ensure_cols(hitters, "hitters")
    _ensure_cols(sps, "starting_pitchers")

    conflicts = detect_conflicts(hitters, sps, cfg)
    if conflicts.empty:
        conflicts["action"] = "none"
        if cfg.drop_policy == "soft_penalty":
            hitters = hitters.copy()
            sps = sps.copy()
            hitters["tier_adjusted"] = hitters["tier"]
            sps["tier_adjusted"] = sps["tier"]
        return hitters, sps, conflicts

    if cfg.drop_policy in ("prefer_sp", "prefer_hitters"):
        drop_hitters_ids = set()
        drop_sps_ids = set()

        grouped = conflicts.groupby(
            ["player_id_sp", "name_sp", "team_sp", "opponent_team_sp", "tier_sp"], as_index=False
        )

        actions = []
        for sp_key, g in grouped:
            sp_id, sp_name, sp_team, sp_opp, sp_tier = sp_key
            g_sorted = g.sort_values("tier_h", ascending=False)
            high_tier_count = (g_sorted["tier_h"] >= cfg.hitter_tier_min).sum()

            if cfg.drop_policy == "prefer_sp":
                # Base rule: drop hitters strictly lower than this SP
                for _, row in g_sorted.iterrows():
                    if pd.notna(row["tier_h"]) and pd.notna(sp_tier) and row["tier_h"] < sp_tier:
                        drop_hitters_ids.add(row["player_id_h"])
                        actions.append(("drop_hitter", sp_id, row["player_id_h"]))

                # NEW cap vs top-tier SPs
                if pd.notna(sp_tier) and sp_tier >= cfg.top_sp_tier_min:
                    # Candidates to KEEP must pass both thresholds
                    keep_candidates = g_sorted[
                        (g_sorted["tier_h"] >= cfg.hitter_tier_min) &
                        (g_sorted["tier_h"] >= (sp_tier + cfg.hitter_over_sp_margin))
                    ].copy()

                    if keep_candidates.empty and cfg.min_keep_vs_top_sp > 0:
                        # Allow occasional one-offs even if margin filter yields none
                        keep_candidates = g_sorted.head(cfg.min_keep_vs_top_sp).copy()

                    # Limit to max_keep
                    max_keep = max(int(cfg.max_hitters_vs_top_sp), 0)
                    keep_ids = set(keep_candidates.head(max_keep)["player_id_h"].tolist()) if max_keep > 0 else set()

                    # Drop all other hitters (even if their tier >= SP tier)
                    for _, row in g_sorted.iterrows():
                        hid = row["player_id_h"]
                        if hid not in keep_ids:
                            drop_hitters_ids.add(hid)
                            actions.append(("drop_hitter_by_cap", sp_id, hid))

                # Optional: if many legit hitters meet threshold, drop SP
                if high_tier_count >= cfg.drop_sp_if_n_hit_gte:
                    drop_sps_ids.add(sp_id)
                    actions.append(("drop_sp", sp_id, None))

            else:  # prefer_hitters
                if high_tier_count >= 1:
                    drop_sps_ids.add(sp_id)
                    actions.append(("drop_sp", sp_id, None))

        hitters_f = hitters[~hitters["player_id"].isin(drop_hitters_ids)].copy()
        sps_f = sps[~sps["player_id"].isin(drop_sps_ids)].copy()

        def _action_for_pair(r):
            h_drop = r["player_id_h"] in drop_hitters_ids
            s_drop = r["player_id_sp"] in drop_sps_ids
            if h_drop and s_drop:
                return "drop_both"
            if h_drop:
                # distinguish cap vs base drop if possible
                return "drop_hitter_by_cap" if ("drop_hitter_by_cap", r["player_id_sp"], r["player_id_h"]) in actions else "drop_hitter"
            if s_drop:
                return "drop_sp"
            return "keep_both"

        conflicts["action"] = conflicts.apply(_action_for_pair, axis=1)
        return hitters_f, sps_f, conflicts

    # soft_penalty
    hitters_f = hitters.copy()
    sps_f = sps.copy()

    hitter_pen_ct = conflicts.groupby("player_id_h")["player_id_sp"].count().rename("n_conflicts")
    sp_pen_ct = conflicts.groupby("player_id_sp")["player_id_h"].count().rename("n_conflicts")

    hitters_f = hitters_f.merge(hitter_pen_ct, left_on="player_id", right_index=True, how="left").fillna({"n_conflicts": 0})
    sps_f = sps_f.merge(sp_pen_ct, left_on="player_id", right_index=True, how="left").fillna({"n_conflicts": 0})

    hitters_f["tier_adjusted"] = hitters_f["tier"] - (hitters_f["n_conflicts"] * cfg.conflict_tier_penalty)
    sps_f["tier_adjusted"] = sps_f["tier"] - (sps_f["n_conflicts"] * cfg.conflict_tier_penalty)

    # cap total drop
    max_drop = float(cfg.conflict_tier_penalty_max)
    hitters_f["tier_adjusted"] = hitters_f[["tier", "tier_adjusted"]].apply(
        lambda r: r["tier"] - max_drop if r["tier"] - r["tier_adjusted"] > max_drop else r["tier_adjusted"], axis=1
    )
    sps_f["tier_adjusted"] = sps_f[["tier", "tier_adjusted"]].apply(
        lambda r: r["tier"] - max_drop if r["tier"] - r["tier_adjusted"] > max_drop else r["tier_adjusted"], axis=1
    )

    hitters_f["tier_adjusted"] = hitters_f["tier_adjusted"].clip(lower=0, upper=10)
    sps_f["tier_adjusted"] = sps_f["tier_adjusted"].clip(lower=0, upper=10)

    conflicts["action"] = "penalize_both"
    return hitters_f, sps_f, conflicts
