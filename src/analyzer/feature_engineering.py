#!/usr/bin/env python3
"""
feature_engineering.py

Compute rolling performance metrics (3/6/10-game windows) and merge in contextual features
(weather, matchups) into a unified DataFrame ready for streak detection and ranking.
"""
import pandas as pd
from pathlib import Path

def compute_rolling_stats(log_df: pd.DataFrame, windows=(3, 6, 10)) -> pd.DataFrame:
    """
    Given a game-log DataFrame with one row per player-game,
    compute rolling batting average, on-base pct, slugging, etc.
    over the specified window sizes (in number of recent games).

    Parameters:
    - log_df: DataFrame with columns ['player_id', 'date', 'box_score'] and other fields
    - windows: iterable of integers, window sizes (e.g. [3,6,10])

    Returns:
    - DataFrame where for each player-game, the rolling metrics for each window are appended
    """
    # Normalize date and sort
    df = log_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['player_id','date'])

    # Extract basic counting stats into flat columns
    stats = ['hits','at_bats','runs','home_runs','rbi']  # extend as needed
    flat = pd.json_normalize(df['box_score'])
    flat.index = df.index
    df = pd.concat([df, flat[stats]], axis=1)

    # Compute rolling, by player
    for w in windows:
        roll = df.groupby('player_id')[stats].rolling(window=w, min_periods=1).sum()
        roll = roll.reset_index(level=0, drop=True)
        for stat in stats:
            df[f'{stat}_sum_last_{w}'] = roll[stat]
        df[f'games_played_last_{w}'] = df.groupby('player_id')['date'].rolling(window=w, min_periods=1).count().reset_index(level=0, drop=True)
        # compute averages (e.g. batting average)
        df[f'avg_last_{w}'] = df[f'hits_sum_last_{w}'] / df[f'at_bats_sum_last_{w}'].replace(0, pd.NA)

    return df


def merge_context(df: pd.DataFrame, structured_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge game-log rolling stats with structured player info
    (handedness, weather_context, betting_context) for the same date.

    Parameters:
    - df: DataFrame output from compute_rolling_stats
    - structured_df: DataFrame loaded from structured_players_{date}.json with player_id and context columns

    Returns:
    - Merged DataFrame ready for streak analysis and ranking
    """
    # Ensure same date type
    structured_df['date'] = pd.to_datetime(structured_df['date'])
    df = df.merge(
        structured_df[['player_id', 'date', 'weather_context', 'betting_context', 'home_or_away', 'opponent_team']],
        on=['player_id','date'], how='left'
    )
    return df

if __name__ == '__main__':
    import argparse
    from analyzer.data_loader import load_game_log, load_structured_players

    parser = argparse.ArgumentParser(description='Compute rolling stats and merge context')
    parser.add_argument('--archive', type=Path, required=True, help='Path to player_game_log.jsonl')
    parser.add_argument('--structured', type=Path, required=True, help='Path to structured_players_{date}.json')
    parser.add_argument('--date', type=str, required=True, help='Date in YYYY-MM-DD format')
    args = parser.parse_args()

    log_df = load_game_log(args.archive)
    structured_df = load_structured_players(args.structured)

    df_roll = compute_rolling_stats(log_df)
    df_ctx = merge_context(df_roll, structured_df)
    print(f"☑️ Computed features for {len(df_ctx)} player-games.")
