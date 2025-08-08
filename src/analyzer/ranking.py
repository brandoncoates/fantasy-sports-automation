#!/usr/bin/env python3
"""
ranking.py

Apply tier-based ranking to players based on weighted features and streak info.
"""
import pandas as pd
from pathlib import Path

# You can import your trend_weights mapping here
# e.g., from trend_weights import FEATURE_WEIGHTS

FEATURE_WEIGHTS = {
    # Example weights; replace with your tuned values
    'avg_last_3': 1.0,
    'avg_last_6': 1.0,
    'avg_last_10': 1.0,
    'current_streak_length': 0.5,
    'weather.temperature_f': 0.2,
    'betting_context.over_under': 0.1,
    # add more feature keys as needed
}


def assign_tiers(df: pd.DataFrame, weights: dict = FEATURE_WEIGHTS) -> pd.DataFrame:
    """
    Compute a raw score for each player-game by combining features and weights,
    then map the score to a tier 0-10.

    Parameters:
    - df: DataFrame with numerical feature columns matching keys in weights
    - weights: dict mapping feature names to weight (float)

    Returns:
    - DataFrame with added 'raw_score' and 'tier' columns.
    """
    df = df.copy()
    # Compute weighted sum
    df['raw_score'] = 0.0
    for feature, weight in weights.items():
        # support nested dicts stored as dicts in columns
        if '.' in feature:
            col, sub = feature.split('.', 1)
            df['raw_score'] += df[col].apply(lambda x: x.get(sub, 0) if isinstance(x, dict) else 0) * weight
        else:
            if feature in df.columns:
                df['raw_score'] += df[feature].fillna(0) * weight

    # Normalize raw_score to 0-10 scale
    min_score = df['raw_score'].min()
    max_score = df['raw_score'].max()
    if max_score > min_score:
        df['tier'] = ((df['raw_score'] - min_score) / (max_score - min_score) * 10).round().astype(int)
    else:
        df['tier'] = 5  # default neutral if no variation

    df['tier'] = df['tier'].clip(0, 10)
    return df

if __name__ == '__main__':
    import argparse
    from analyzer.data_loader import load_game_log, load_structured_players
    from analyzer.feature_engineering import compute_rolling_stats, merge_context
    from analyzer.streaks import annotate_streaks

    parser = argparse.ArgumentParser(description='Assign tiers 0-10 to player-games')
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--structured', type=Path, required=True)
    parser.add_argument('--date', type=str, required=True)
    args = parser.parse_args()

    # Load and prepare data
    logs = load_game_log(args.archive)
    structured = load_structured_players(args.structured)
    feats = compute_rolling_stats(logs)
    ctx = merge_context(feats, structured)
    streaked = annotate_streaks(ctx)

    # Assign tiers
    ranked = assign_tiers(streaked)
    print(f"Assigned tiers for {len(ranked)} player-games.")
    # Optionally, save to CSV
    out_path = Path(__file__).resolve().parent.parent / f"tiers_{args.date}.csv"
    ranked[['player_id','name','date','raw_score','tier']].to_csv(out_path, index=False)
    print(f"Saved tiers to {out_path}")
