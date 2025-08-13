#!/usr/bin/env python3
"""
cli.py

Run the full analysis pipeline: load data, engineer features,
annotate streaks, assign tiers, and evaluate performance.

Outputs (under data/analysis/):
  - tiers_raw_<DATE>.csv                # all players, with context columns
  - ranked_full_<DATE>.csv              # predictions JOIN actuals (+context) for learning
  - tiers_position_players_<DATE>.csv   # non-pitchers only
  - tiers_hitters_<DATE>.csv            # alias of tiers_position_players_* (workflow compatibility)
  - tiers_starting_pitchers_<DATE>.csv  # probable SPs only (if detectable)
  - evaluation_<DATE>.csv               # evaluation summary
  - calibration_<DATE>.csv              # tier bucket hit-rate (if actuals available)
  - eval_history.jsonl                  # rolling per-row learning records (appended daily)
  - (optional) matching .parquet files if pyarrow is installed

New (when --resolve-conflicts is used):
  - tiers_hitters_filtered_<DATE>.csv
  - tiers_starting_pitchers_filtered_<DATE>.csv
  - tiers_conflicts_<DATE>.csv
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

import pandas as pd

from analyzer.data_loader import load_game_log, load_structured_players
from analyzer.feature_engineering import compute_rolling_stats, merge_context
from analyzer.streaks import annotate_streaks
from analyzer.ranking import assign_tiers
from analyzer.evaluation import evaluate_predictions
from analyzer.conflicts import ConflictConfig, resolve_conflicts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def _ensure_name(ranked_df: pd.DataFrame, struct_df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee a 'name' column exists, pulling from common variants or struct_df if needed."""
    df = ranked_df.copy()
    if "name" in df.columns:
        return df

    # Try common variants first
    for cand in ["player_name", "full_name", "player_full_name", "display_name", "name_x", "name_y"]:
        if cand in df.columns:
            df = df.rename(columns={cand: "name"})
            logger.info(f"Renamed {cand} -> name")
            return df

    # Merge from structured on player_id if possible
    if "player_id" in df.columns and "name" in struct_df.columns:
        names = struct_df[["player_id", "name"]].drop_duplicates()
        before = len(df)
        df = df.merge(names, on="player_id", how="left", suffixes=("", "_struct"))
        after = len(df)
        logger.info(f"Merged names from structured: {before} -> {after} rows")
        if "name" in df.columns:
            return df

    # Final fallback: synthesize a name from player_id or index
    src = df["player_id"].astype(str) if "player_id" in df.columns \
          else pd.Series(df.index, index=df.index).astype(str)
    df["name"] = src
    logger.warning("Backfilled 'name' from player_id/index")
    return df


def main():
    parser = argparse.ArgumentParser(description="Run full player trend analysis pipeline")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--archive",
        type=Path,
        default=Path("player_game_log.jsonl"),
        help="Path to player game log JSONL (default: ./player_game_log.jsonl)",
    )
    parser.add_argument(
        "--structured",
        type=Path,
        default=None,
        help="Path to structured players JSON (default: structured_players_<DATE>.json)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analysis"),
        help="Directory to write outputs (default: data/analysis/)",
    )
    parser.add_argument(
        "--no-parquet",
        action="store_true",
        help="Skip writing parquet outputs even if pyarrow is available.",
    )

    # --- Conflict resolver knobs ---
    parser.add_argument("--resolve-conflicts", action="store_true",
                        help="Run SP↔hitter conflict resolver after ranking and write filtered outputs.")
    parser.add_argument("--drop-policy", type=str,
                        default=os.getenv("DROP_POLICY", "prefer_sp"),
                        choices=["prefer_sp", "prefer_hitters", "soft_penalty"],
                        help="Conflict policy (env DROP_POLICY also supported).")
    parser.add_argument("--hitter-tier-min", type=float,
                        default=float(os.getenv("HITTER_TIER_MIN", 6.0)),
                        help="Minimum hitter tier to treat as legit in conflicts.")
    parser.add_argument("--sp-tier-min", type=float,
                        default=float(os.getenv("SP_TIER_MIN", 6.0)),
                        help="Minimum SP tier to treat as legit in conflicts.")
    parser.add_argument("--drop-sp-if-n-hit-gte", type=int,
                        default=int(os.getenv("DROP_SP_IF_N_HIT_GTE", 2)),
                        help="Only in prefer_sp: drop SP if >=N opposing hitters meet threshold.")
    parser.add_argument("--conflict-tier-penalty", type=float,
                        default=float(os.getenv("CONFLICT_TIER_PENALTY", 0.4)),
                        help="soft_penalty: per-conflict tier penalty.")
    parser.add_argument("--conflict-tier-penalty-max", type=float,
                        default=float(os.getenv("CONFLICT_TIER_PENALTY_MAX", 1.0)),
                        help="soft_penalty: cap on total tier penalty.")

    args = parser.parse_args()

    if args.structured is None:
        args.structured = Path(f"structured_players_{args.date}.json")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"📦 Archive:    {args.archive.resolve()}")
    print(f"📦 Structured: {args.structured.resolve()}")
    print(f"📂 Outputs:    {args.output_dir.resolve()}")

    # ---- Load
    log_df = load_game_log(args.archive)
    struct_df = load_structured_players(args.structured)
    print(f"🔢 Loaded game log rows: {len(log_df)}")
    print(f"🔢 Loaded structured rows: {len(struct_df)}")

    # Ensure structured has a date column (daily snapshot)
    if "date" not in struct_df.columns:
        struct_df = struct_df.copy()
        struct_df["date"] = args.date

    # ---- Features
    feat_df = compute_rolling_stats(log_df)
    print(f"🧪 Feature rows: {len(feat_df)}")

    # Cold start: seed from structured so we can still rank
    if feat_df.empty:
        print("🧊 No game-log history — cold start mode. Seeding from structured players.")
        seed_cols = [c for c in ["player_id", "name", "date"] if c in struct_df.columns]
        feat_df = struct_df[seed_cols].copy()
        dd_subset = [c for c in ["player_id", "date"] if c in feat_df.columns]
        feat_df = feat_df.drop_duplicates(subset=dd_subset) if dd_subset else feat_df.drop_duplicates()
        for col in ("avg_last_3", "avg_last_6", "avg_last_10", "current_streak_length"):
            if col not in feat_df.columns:
                feat_df[col] = 0

    # ---- Context merge
    ctx_df = merge_context(feat_df, struct_df)
    print(f"🧩 Context-merged rows: {len(ctx_df)}")

    # ---- Streaks & Ranking
    streaked_df = annotate_streaks(ctx_df)
    print(f"🔥 Streaks annotated rows: {len(streaked_df)}")

    ranked_df = assign_tiers(streaked_df)
    print(f"🏅 Ranked rows: {len(ranked_df)}")

    # ---- Evaluation (safe if empty)
    eval_df = evaluate_predictions(ranked_df, log_df)
    print(f"✅ Evaluation rows: {len(eval_df)}")

    # ---- Derive starting pitcher flag
    if "position" in ranked_df.columns and ("is_probable_starter" in ranked_df.columns or "starter" in ranked_df.columns):
        starter_col = "is_probable_starter" if "is_probable_starter" in ranked_df.columns else "starter"
        ranked_df["starting_pitcher_today"] = (
            ranked_df["position"].isin(["P", "SP", "RP"]) & ranked_df[starter_col].fillna(False)
        )
    else:
        ranked_df["starting_pitcher_today"] = False

    # ---- Ensure 'name' exists
    ranked_df = _ensure_name(ranked_df, struct_df)

    # ---- Export columns
    base_cols = ["player_id", "name", "date", "raw_score", "tier"]
    context_cols = ["team", "opponent_team", "home_or_away", "position", "starting_pitcher_today"]
    must_haves = ["player_id", "name", "date", "raw_score", "tier"]

    # Fill missing date defensively
    if "date" not in ranked_df.columns:
        ranked_df["date"] = args.date

    missing = [c for c in must_haves if c not in ranked_df.columns]
    if missing:
        raise KeyError(f"Missing expected columns in ranked_df: {missing}")

    out_cols = [c for c in (base_cols + context_cols) if c in ranked_df.columns]

    # ---- RAW (all players)
    raw_csv = args.output_dir / f"tiers_raw_{args.date}.csv"
    ranked_df[out_cols].to_csv(raw_csv, index=False)
    print(f"💾 Wrote RAW tiers CSV to {raw_csv}")

    # ---- LEARNING SNAPSHOT (predictions JOIN actuals)
    actuals = log_df[["player_id", "date", "hits"]] if "hits" in log_df.columns else pd.DataFrame(columns=["player_id", "date", "hits"])
    learn_df = ranked_df.merge(actuals, how="left", on=["player_id", "date"])
    learn_df["hits"] = learn_df["hits"].fillna(0)
    learn_df["hit_flag"] = (learn_df["hits"] > 0).astype(int)

    ranked_full_csv = args.output_dir / f"ranked_full_{args.date}.csv"
    learn_df[out_cols + ["hits", "hit_flag"]].to_csv(ranked_full_csv, index=False)
    print(f"💾 Wrote full learning snapshot to {ranked_full_csv}")

    # ---- Parquet (optional)
    if not args.no_parquet:
        try:
            ranked_df[out_cols].to_parquet(args.output_dir / f"tiers_raw_{args.date}.parquet", index=False)
            learn_df[out_cols + ["hits", "hit_flag"]].to_parquet(args.output_dir / f"ranked_full_{args.date}.parquet", index=False)
        except Exception as e:
            logger.info(f"Skipped Parquet writes: {e}")

    # ---- Position players (hitters)
    hitters = ranked_df[~ranked_df.get("position", pd.Series(index=ranked_df.index)).isin(["P", "SP", "RP"])].copy()
    if not hitters.empty:
        hitters_csv = args.output_dir / f"tiers_position_players_{args.date}.csv"
        hitters[out_cols].to_csv(hitters_csv, index=False)
        print(f"💾 Wrote hitters tiers CSV to {hitters_csv}")

        alias_hitters = args.output_dir / f"tiers_hitters_{args.date}.csv"
        hitters[out_cols].to_csv(alias_hitters, index=False)
        print(f"💾 Wrote alias tiers_hitters CSV to {alias_hitters}")
    else:
        print("ℹ️ No hitters found to export.")
        alias_hitters = None

    # ---- Probable starting pitchers
    starters = ranked_df[
        ranked_df.get("starting_pitcher_today", pd.Series(False, index=ranked_df.index)).fillna(False)
        & ranked_df.get("position", pd.Series(index=ranked_df.index)).isin(["P", "SP", "RP"])
    ].copy()
    if not starters.empty:
        sp_csv = args.output_dir / f"tiers_starting_pitchers_{args.date}.csv"
        starters[out_cols].to_csv(sp_csv, index=False)
        print(f"💾 Wrote probable starters tiers CSV to {sp_csv}")
    else:
        print("ℹ️ No starting pitchers found to export.")
        sp_csv = None

    # ---- Evaluation file
    eval_csv = args.output_dir / f"evaluation_{args.date}.csv"
    eval_df.to_csv(eval_csv, index=False)
    print(f"💾 Wrote evaluation CSV to {eval_csv}")

    # ---- Append learning records to rolling JSONL
    try:
        hist_path = args.output_dir / "eval_history.jsonl"
        keep_for_jsonl = [
            "date", "player_id", "name", "position", "team", "opponent_team", "home_or_away",
            "tier", "raw_score", "hits", "hit_flag", "starting_pitcher_today",
            "betting_context", "weather_context"
        ]
        jcols = [c for c in keep_for_jsonl if c in learn_df.columns]
        with hist_path.open("a", encoding="utf-8") as f:
            for rec in learn_df[jcols].to_dict(orient="records"):
                f.write(json.dumps(rec, default=str) + "\n")
        print(f"🧷 Appended {len(learn_df)} learning records to {hist_path}")
    except Exception as e:
        logger.info(f"Skipped eval history append: {e}")

    # ---- Quick calibration summary if we have actuals
    try:
        if "hits" in log_df.columns and not ranked_df.empty:
            calib = ranked_df.merge(
                log_df[["player_id", "date", "hits"]], how="left", on=["player_id", "date"]
            )
            if not calib.empty:
                calib["hit_flag"] = (calib["hits"].fillna(0) > 0).astype(int)
                calib["tier_bucket"] = pd.cut(
                    calib["tier"], bins=[-1, 2, 5, 8, 11], labels=["low", "mid", "high", "elite"]
                )
                calib_summary = (
                    calib.groupby("tier_bucket", dropna=False)
                         .agg(games=("hit_flag", "size"), hit_rate=("hit_flag", "mean"))
                         .reset_index()
                )
                calib_path = args.output_dir / f"calibration_{args.date}.csv"
                calib_summary.to_csv(calib_path, index=False)
                print(f"📈 Wrote calibration to {calib_path}")
    except Exception as e:
        logger.info(f"Skipped calibration: {e}")

    # ---- Conflict resolution (after CSVs exist)
    try:
        if args.resolve_conflicts and alias_hitters and sp_csv:
            # Load the just-written per-side CSVs
            hitters_df = pd.read_csv(alias_hitters)
            sp_df = pd.read_csv(sp_csv)

            # Basic column checks
            req = ["player_id", "name", "team", "opponent_team", "tier"]
            for need in req:
                if need not in hitters_df.columns:
                    raise RuntimeError(f"hitters CSV missing '{need}'")
                if need not in sp_df.columns:
                    raise RuntimeError(f"SP CSV missing '{need}'")

            cfg = ConflictConfig(
                hitter_tier_min=args.hitter_tier_min,
                sp_tier_min=args.sp_tier_min,
                drop_policy=args.drop_policy,
                drop_sp_if_n_hit_gte=args.drop_sp_if_n_hit_gte,
                conflict_tier_penalty=args.conflict_tier_penalty,
                conflict_tier_penalty_max=args.conflict_tier_penalty_max,
            )

            hitters_f, sp_f, conflicts_df = resolve_conflicts(hitters_df, sp_df, cfg)

            # Write outputs
            hitters_out = args.output_dir / f"tiers_hitters_filtered_{args.date}.csv"
            sp_out = args.output_dir / f"tiers_starting_pitchers_filtered_{args.date}.csv"
            conflicts_out = args.output_dir / f"tiers_conflicts_{args.date}.csv"

            hitters_f.to_csv(hitters_out, index=False)
            sp_f.to_csv(sp_out, index=False)
            conflicts_df.to_csv(conflicts_out, index=False)

            print(f"🧹 Conflict resolver wrote: {hitters_out.name}, {sp_out.name}, {conflicts_out.name}")
        elif args.resolve_conflicts:
            print("ℹ️ Conflict resolver enabled but missing hitters or SP CSVs — skipped.")
        else:
            print("ℹ️ Conflict resolver not enabled. Use --resolve-conflicts to run it.")
    except Exception as e:
        logger.info(f"Conflict resolver step skipped due to error: {e}")


if __name__ == "__main__":
    main()
