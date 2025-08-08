#!/usr/bin/env python3
"""
streaks.py

Detect hitting and slump streaks for players and annotate each game entry with current streak status.
"""
import pandas as pd
import numpy as np

def annotate_streaks(df: pd.DataFrame, stat_column: str = 'hits', date_column: str = 'date') -> pd.DataFrame:
    """
    Given a DataFrame sorted by player_id and date, compute streaks:
      - A positive streak when stat_column > 0 (e.g., hit streak)
      - A negative streak when stat_column == 0 (e.g., slump)
    Annotates each row with:
      - current_streak_length: int (positive for streak, negative for slump)
      - streak_type: 'hot' or 'cold'
      - streak_position: how far into the current streak

    Parameters:
    - df: DataFrame with columns ['player_id', date_column, stat_column]
    - stat_column: column to evaluate (default 'hits')
    - date_column: column representing game date

    Returns:
    - df with appended columns ['current_streak_length', 'streak_type', 'streak_position']
    """
    df = df.copy()
    streaks = []

    for pid, group in df.groupby('player_id'):
        current_streak = 0
        positions = []
        types = []
        streak_lens = []

        for val in group[stat_column]:
            # Determine if hot or cold
            if val > 0:
                # continuing or starting a hot streak
                if current_streak >= 0:
                    current_streak += 1
                else:
                    current_streak = 1
                streak_type = 'hot'
            else:
                # val == 0 -> cold streak
                if current_streak <= 0:
                    current_streak -= 1
                else:
                    current_streak = -1
                streak_type = 'cold'

            streak_lens.append(current_streak)
            types.append(streak_type)
            # position is absolute value within streak
            positions.append(abs(current_streak))

        streaks.extend(
            zip(streak_lens, types, positions)
        )

    # insert into DataFrame
    df['current_streak_length'] = [s[0] for s in streaks]
    df['streak_type'] = [s[1] for s in streaks]
    df['streak_position'] = [s[2] for s in streaks]

    return df

if __name__ == '__main__':
    import argparse
    from analyzer.feature_engineering import compute_rolling_stats, merge_context
    from analyzer.data_loader import load_game_log, load_structured_players
    
    parser = argparse.ArgumentParser(description='Annotate player-game DataFrame with streak info')
    parser.add_argument('--archive', type=Path, required=True, help='Path to game log JSONL')
    parser.add_argument('--structured', type=Path, required=True, help='Path to structured players JSON')
    parser.add_argument('--date', type=str, required=True)
    args = parser.parse_args()

    log_df = load_game_log(args.archive)
    struct_df = load_structured_players(args.structured)
    feat_df = compute_rolling_stats(log_df)
    ctx_df = merge_context(feat_df, struct_df)
    streak_df = annotate_streaks(ctx_df)
    print(f"Annotated streaks for {len(streak_df)} records.")
