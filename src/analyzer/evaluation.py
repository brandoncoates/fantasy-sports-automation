#!/usr/bin/env python3
"""
evaluation.py

Compare predicted tiers against actual performance to identify hits and misses for further tuning.
"""
import pandas as pd
from pathlib import Path


def evaluate_predictions(pred_df: pd.DataFrame, log_df: pd.DataFrame, tier_col: str = 'tier', stat_col: str = 'hits') -> pd.DataFrame:
    """
    Compare the predicted tiers in pred_df against actual performance in log_df.
    Compute metrics such as:
      - Hits: high-tier (>=8) players who performed well (e.g. stat_col > 0)
      - Misses: high-tier players who underperformed (stat_col == 0)
      - False negatives: low-tier (<=2) who overperformed
    Returns a summary DataFrame of misclassifications for review.

    Parameters:
    - pred_df: DataFrame containing player_id, date, tier, and stat_col
    - log_df: DataFrame containing actual game-log with stat_col
    - tier_col: column name for predicted tier
    - stat_col: column name for actual performance (e.g. 'hits')

    Returns:
    - pd.DataFrame with columns [player_id, date, tier, actual_{stat_col}, category, details]
    """
    df = pred_df.merge(
        log_df[['player_id', 'date', stat_col]],
        on=['player_id', 'date'],
        how='left'
    )

    results = []
    for _, row in df.iterrows():
        tier = row[tier_col]
        actual = row[stat_col]
        category = None

        if tier >= 8 and actual == 0:
            category = 'miss_high'
        elif tier <= 2 and actual > 0:
            category = 'false_negative'
        elif tier >= 8 and actual > 0:
            category = 'hit_high'
        elif tier <= 2 and actual == 0:
            category = 'hit_low'

        if category:
            results.append({
                'player_id': row['player_id'],
                'date': row['date'],
                'tier': tier,
                f'actual_{stat_col}': actual,
                'category': category
            })

    return pd.DataFrame(results)

if __name__ == '__main__':
    import argparse
    from analyzer.data_loader import load_game_log, load_structured_players
    from analyzer.feature_engineering import compute_rolling_stats, merge_context
    from analyzer.streaks import annotate_streaks
    from analyzer.ranking import assign_tiers
    from pathlib import Path

    parser = argparse.ArgumentParser(description='Evaluate tier predictions against actual performance')
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--structured', type=Path, required=True)
    parser.add_argument('--date', type=str, required=True)
    args = parser.parse_args()

    # Load and prepare data
    log_df = load_game_log(args.archive)
    structured_df = load_structured_players(Path(f'structured_players_{args.date}.json'))
    feats = compute_rolling_stats(log_df)
    ctx = merge_context(feats, structured_df)
    streaked = annotate_streaks(ctx)
    ranked = assign_tiers(streaked)

    # Evaluate
    eval_df = evaluate_predictions(ranked, log_df)
    out_path = Path(__file__).resolve().parent.parent / f'evaluation_{args.date}.csv'
    eval_df.to_csv(out_path, index=False)
    print(f"✅ Wrote evaluation to {out_path}")
