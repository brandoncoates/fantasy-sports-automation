#!/usr/bin/env python3
"""
cli.py

Run the full analysis pipeline: load data, engineer features,
annotate streaks, assign tiers, and evaluate performance.
"""

import argparse
from pathlib import Path

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

    # 2) Feature engineering (rolling stats from history)
    feat_df = compute_rolling_stats(log_df)
    print(f"🧪 Feature rows: {len(feat_df)}")

    # 3) Merge structured context (weather/betting/home_or_away/opponent_team/position)
    ctx_df = merge_context(feat_df, struct_df)
    print(f"🧩 Context-merged rows: {len(ctx_df)}")

    # 4) Streak annotation (adds batter_*/pitcher_* + unified streak columns)
    streaked_df = annotate_streaks(ctx_df)
    print(f"🔥 Streaks annotated rows: {len(streaked_df)}")

    # 5) Rank assignments
    ranked_df = assign_tiers(streaked_df)
    print(f"🏅 Ranked rows: {len(ranked_df)}")

    # 6) Evaluation (compare against actuals in game log)
    eval_df = evaluate_predictions(ranked_df, log_df)
    print(f"✅ Evaluation rows: {len(eval_df)}")

    # Write outputs
    tier_cols = ["player_id", "name", "date", "raw_score", "tier"]
    missing_cols = [c for c in tier_cols if c not in ranked_df.columns]
    if missing_cols:
        raise KeyError(f"Missing expected columns in ranked_df: {missing_cols}")

    tier_csv = args.output_dir / f"tiers_{args.date}.csv"
    ranked_df[tier_cols].to_csv(tier_csv, index=False)
    print(f"💾 Wrote tiers CSV to {tier_csv}")

    eval_csv = args.output_dir / f"evaluation_{args.date}.csv"
    eval_df.to_csv(eval_csv, index=False)
    print(f"💾 Wrote evaluation CSV to {eval_csv}")

    # Also write Parquet (optional; requires pyarrow)
    try:
        tier_parq = args.output_dir / f"tiers_{args.date}.parquet"
        ranked_df[tier_cols].to_parquet(tier_parq, index=False)
        print(f"💾 Wrote tiers Parquet to {tier_parq}")

        eval_parq = args.output_dir / f"evaluation_{args.date}.parquet"
        eval_df.to_parquet(eval_parq, index=False)
        print(f"💾 Wrote evaluation Parquet to {eval_parq}")
    except Exception as e:
        print(f"⚠️ Skipped Parquet writes (install pyarrow to enable): {e}")


if __name__ == "__main__":
    main()
