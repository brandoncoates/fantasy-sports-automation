#!/usr/bin/env python3
"""
cli.py

Command-line interface to run the full analysis pipeline: load data, engineer features,
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
    parser = argparse.ArgumentParser(description='Run full player trend analysis pipeline')
    parser.add_argument('--date', type=str, required=True, help='Date in YYYY-MM-DD format')
    parser.add_argument('--archive', type=Path, help='Path to game log JSONL',
                        default=Path('data/archive/player_game_log.jsonl'))
    parser.add_argument('--structured', type=Path, help='Path to structured players JSON',
                        default=Path(f'structured_players_{parser.parse_known_args()[0].date}.json'))
    parser.add_argument('--output-dir', type=Path, help='Directory to write outputs',
                        default=Path('data/analysis'))
    args = parser.parse_args()

    # Ensure output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Load data
    log_df = load_game_log(args.archive)
    struct_df = load_structured_players(args.structured)

    # Step 2: Feature engineering
    feat_df = compute_rolling_stats(log_df)
    ctx_df = merge_context(feat_df, struct_df)

    # Step 3: Streak annotation
    streaked_df = annotate_streaks(ctx_df)

    # Step 4: Rank assignments
    ranked_df = assign_tiers(streaked_df)

    # Step 5: Evaluation
    eval_df = evaluate_predictions(ranked_df, log_df)

    # Write tier output
    tier_path = args.output_dir / f'tiers_{args.date}.csv'
    ranked_df[['player_id','name','date','raw_score','tier']].to_csv(tier_path, index=False)
    print(f"✅ Wrote tiers to {tier_path}")

    # Write evaluation output
    eval_path = args.output_dir / f'evaluation_{args.date}.csv'
    eval_df.to_csv(eval_path, index=False)
    print(f"✅ Wrote evaluation to {eval_path}")

if __name__ == '__main__':
    main()
