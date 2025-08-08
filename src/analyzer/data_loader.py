#!/usr/bin/env python3
"""
data_loader.py

Load the append-only game log archive and today’s structured players JSON into pandas DataFrames for analysis.
"""
import json
from pathlib import Path
import pandas as pd

def load_game_log(archive_path: Path) -> pd.DataFrame:
    """
    Read the JSONL archive file at archive_path and return a DataFrame where each row is one game-log entry.

    Parameters:
    - archive_path: Path to `player_game_log.jsonl` (append-only archive)

    Returns:
    - pandas.DataFrame with one row per game-log entry.
    """
    records = []
    with archive_path.open('r', encoding='utf-8') as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return pd.DataFrame(records)


def load_structured_players(structured_path: Path) -> pd.DataFrame:
    """
    Read the structured players JSON file at structured_path and return a DataFrame.

    Parameters:
    - structured_path: Path to `structured_players_{date}.json`

    Returns:
    - pandas.DataFrame with one row per player.
    """
    data = json.loads(structured_path.read_text(encoding='utf-8'))
    # Convert nested dicts into flat columns if necessary
    df = pd.json_normalize(data.values())
    return df

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Load game log and structured player data into DataFrames')
    parser.add_argument('--archive', type=Path, required=True, help='Path to player_game_log.jsonl')
    parser.add_argument('--structured', type=Path, required=True, help='Path to structured_players_{date}.json')
    args = parser.parse_args()

    game_log_df = load_game_log(args.archive)
    structured_df = load_structured_players(args.structured)
    print('Loaded {} game-log entries and {} player records.'.format(
        len(game_log_df), len(structured_df)
    ))
