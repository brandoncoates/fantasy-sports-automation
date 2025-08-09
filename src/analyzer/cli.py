#!/usr/bin/env python3
"""
cli.py

Run the full analysis pipeline: load data, engineer features,
annotate streaks, assign tiers, and evaluate performance.
"""

import argparse
from pathlib import Path
import json
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,  # set to DEBUG while fixing if you want more logs
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

from analyzer.data_loader import load_game_log, load_structured_players
from analyzer.feature_engineering import compute_rolling_stats, merge_context
from analyzer.streaks import annotate_streaks
from analyzer.ranking import assign_tiers
from analyzer.evaluation import evaluate_predictions


def main():
    parser = argparse.ArgumentParser(description="Run full player trend analysis pipeline")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")

    # Matches what combine writes:
    #   - player_game_log.jsonl at repo root
    #   - structured_players_<DATE>.json at repo root
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
    args = parser.parse_args()

    # Resolve structured path from --date if not provided
    if args.structured is None:
        args.structured = Path(f"structured_players_{args.date}.json")

    # Ensure output directory exists
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"📦 Archive:    {args.archive.resolve()}")
    print(f"📦 Structured: {args.structured.resolve()}")
    print(f"📂 Outputs:    {args.output_dir.resolve()}")

    # 1) Load data
    log_df = load_game_log(args.archive)
    struct_df = load_structured_players(args.structured)
    print(f"🔢 Loaded game log rows: {len(log_df)}")
    print(f"🔢 Loaded structured rows: {len(struct_df)}")

    # Ensure structured has a date column for today (many structured files are per-day snapshots)
    if "date" not in struct_df.columns:
        struct_df = struct_df.copy()
        struct_df["date"] = args.date

    # 2) Feature engineering (rolling stats from history)
    feat_df = compute_rolling_stats(log_df)
    print(f"🧪 Feature rows: {len(feat_df)}")

    # --- Cold start guard: if no history, seed from structured so we still produce tiers ---
    if feat_df.empty:
        print("🧊 No game-log history found — cold start mode. Seeding from structured players.")
        # Seed with minimal keys (ensure 'name' is carried if available)
        seed_cols = [c for c in ["player_id", "name", "date"] if c in struct_df.columns]
        feat_df = struct_df[seed_cols].copy()

        # Drop duplicates on whatever keys we have available
        dd_subset = [c for c in ["player_id", "date"] if c in feat_df.columns]
        if dd_subset:
            feat_df = feat_df.drop_duplicates(subset=dd_subset)
        else:
            feat_df = feat_df.drop_duplicates()

        # Add zeroed features expected by ranking (safe defaults)
        for col in ("avg_last_3", "avg_last_6", "avg_last_10", "current_streak_length"):
            if col not in feat_df.columns:
                feat_df[col] = 0

    # 3) Merge structured context (weather/betting/home_or_away/opponent_team/position)
    ctx_df = merge_context(feat_df, struct_df)
    print(f"🧩 Context-merged rows: {len(ctx_df)}")

    # 4) Streak annotation (adds batter_*/pitcher_* + unified streak columns)
    streaked_df = annotate_streaks(ctx_df)
    print(f"🔥 Streaks annotated rows: {len(streaked_df)}")

    # 5) Rank assignments
    ranked_df = assign_tiers(streaked_df)
    print(f"🏅 Ranked rows: {len(ranked_df)}")

    # 6) Evaluation (compare against actuals in game log; safe when empty)
    eval_df = evaluate_predictions(ranked_df, log_df)
    print(f"✅ Evaluation rows: {len(eval_df)}")

    # ---------- Derive starting pitcher flag ----------
    # True only if player is a pitcher AND marked as probable starter
    if "position" in ranked_df.columns and ("is_probable_starter" in ranked_df.columns or "starter" in ranked_df.columns):
        starter_col = "is_probable_starter" if "is_probable_starter" in ranked_df.columns else "starter"
        ranked_df["starting_pitcher_today"] = (
            ranked_df["position"].isin(["P", "SP", "RP"]) & ranked_df[starter_col].fillna(False)
        )
    else:
        ranked_df["starting_pitcher_today"] = False

    # ---------- Output: tiers with extra context ----------
    base_cols = ["player_id", "name", "date", "raw_score", "tier"]
    context_cols = ["team", "opponent_team", "home_or_away", "position", "starting_pitcher_today"]

    # --- DEBUG + NORMALIZE before must_haves check ---
    logger.info(f"[DEBUG] ranked_df shape: {ranked_df.shape}")
    logger.info(f"[DEBUG] ranked_df columns: {list(ranked_df.columns)}")

    sample_cols = [c for c in ranked_df.columns if "name" in c.lower() or "player" in c.lower()]
    logger.info(f"[DEBUG] name-like columns present: {sample_cols}")
    if sample_cols:
        try:
            logger.info(f"[DEBUG] ranked_df head (name-like):\n{ranked_df[sample_cols].head(3)}")
        except Exception:
            pass

    # Normalize 'name' if missing but other variants exist
    if "name" not in ranked_df.columns:
        for cand in ["player_name", "full_name", "player_full_name", "display_name", "name_x", "name_y"]:
            if cand in ranked_df.columns:
                ranked_df = ranked_df.rename(columns={cand: "name"})
                logger.warning(f"[DEBUG] Renamed column {cand} -> name")
                break

    # Last resort: map from structured players if we have player_id (FIX: use struct_df, not structured_df)
    if "name" not in ranked_df.columns and "player_id" in ranked_df.columns:
        try:
            if "name" in struct_df.columns:
                sp = struct_df[["player_id", "name"]].drop_duplicates()
                ranked_df = ranked_df.merge(sp, on="player_id", how="left", suffixes=("", "_struct"))
                logger.warning("[DEBUG] Merged name from struct_df by player_id")
        except Exception as e:
            logger.error(f"[DEBUG] Failed to merge name from struct_df: {e}")

    # Final fallback so we never crash downstream
    if "name" not in ranked_df.columns:
        ranked_df["name"] = ranked_df.get("player_id", pd.Series(range(len(ranked_df)))).astype(str)
        logger.warning("[DEBUG] Backfilled 'name' from player_id as string")

    # Must-haves (we expect name now because we seeded/normalized it)
    must_haves = ["player_id", "name", "date", "raw_score", "tier"]
    missing = [c for c in must_haves if c not in ranked_df.columns]
    if missing:
        raise KeyError(f"Missing expected columns in ranked_df: {missing}")

    # Decide which columns we actually write
    out_cols = [c for c in (base_cols + context_cols) if c in ranked_df.columns]

    tier_csv = args.output_dir / f"tiers_{args.date}.csv"
    ranked_df[out_cols].to_csv(tier_csv, index=False)
    print(f"💾 Wrote tiers CSV to {tier_csv}")

    # Extra exports: pitchers-only and probable starters
    if "position" in ranked_df.columns:
        pitchers = ranked_df[ranked_df["position"].isin(["P", "SP", "RP"])].copy()
        if not pitchers.empty:
            pitch_csv = args.output_dir / f"tiers_pitchers_{args.date}.csv"
            pitchers[out_cols].to_csv(pitch_csv, index=False)
            print(f"💾 Wrote pitchers-only tiers CSV to {pitch_csv}")

            if "starting_pitcher_today" in pitchers.columns:
                starters = pitchers[pitchers["starting_pitcher_today"].fillna(False)]
                if not starters.empty:
                    sp_csv = args.output_dir / f"tiers_starting_pitchers_{args.date}.csv"
                    starters[out_cols].to_csv(sp_csv, index=False)
                    print(f"💾 Wrote probable starters tiers CSV to {sp_csv}")

    # ---------- Output: evaluation ----------
    eval_csv = args.output_dir / f"evaluation_{args.date}.csv"
    eval_df.to_csv(eval_csv, index=False)
    print(f"💾 Wrote evaluation CSV to {eval_csv}")

    # Append today’s eval to a rolling JSONL for learning/calibration
    try:
        hist_path = args.output_dir / "eval_history.jsonl"
        with hist_path.open("a", encoding="utf-8") as f:
            for rec in eval_df.to_dict(orient="records"):
                f.write(json.dumps(rec) + "\n")
        print(f"🧷 Appended {len(eval_df)} eval records to {hist_path}")
    except Exception as e:
        print(f"⚠️ Skipped eval history append: {e}")

    # Quick calibration: precision by tier bucket (if we have actuals)
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
        print(f"⚠️ Skipped calibration summary: {e}")

    # Also write Parquet (optional; requires pyarrow)
    try:
        tier_parq = args.output_dir / f"tiers_{args.date}.parquet"
        ranked_df[out_cols].to_parquet(tier_parq, index=False)
        print(f"💾 Wrote tiers Parquet to {tier_parq}")

        eval_parq = args.output_dir / f"evaluation_{args.date}.parquet"
        eval_df.to_parquet(eval_parq, index=False)
        print(f"💾 Wrote evaluation Parquet to {eval_parq}")
    except Exception as e:
        print(f"⚠️ Skipped Parquet writes (install pyarrow to enable): {e}")


if __name__ == "__main__":
    main()
