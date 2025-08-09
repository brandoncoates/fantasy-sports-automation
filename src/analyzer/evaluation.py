#!/usr/bin/env python3
"""
evaluation.py

Compare predicted tiers against actual performance to identify hits and misses
for further tuning. Safe for first-run / no-history cases.
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


def evaluate_predictions(
    pred_df: pd.DataFrame,
    log_df: pd.DataFrame,
    tier_col: str = "tier",
    stat_col: str = "hits",
    high_tier: int = 8,
    low_tier: int = 2,
) -> pd.DataFrame:
    """
    Compare the predicted tiers in pred_df against actual performance in log_df.

    Categories:
      - miss_high:      high-tier (>= high_tier) but actual == 0
      - false_negative: low-tier  (<= low_tier)  but actual > 0
      - hit_high:       high-tier (>= high_tier) and actual > 0
      - hit_low:        low-tier  (<= low_tier)  and actual == 0

    Returns a DataFrame with columns:
      ['player_id', 'date', 'tier', f'actual_{stat_col}', 'category']
      (Empty DataFrame if there are no actuals to compare.)
    """
    # Expected columns
    required_pred = {"player_id", "date", tier_col}
    required_log = {"player_id", "date", stat_col}

    # Guards for empty inputs
    if pred_df is None or pred_df.empty:
        print("⚠️ No predictions provided; returning empty evaluation.")
        return pd.DataFrame(columns=["player_id", "date", "tier", f"actual_{stat_col}", "category"])

    if log_df is None or log_df.empty:
        print("⚠️ No game log available; returning empty evaluation.")
        return pd.DataFrame(columns=["player_id", "date", "tier", f"actual_{stat_col}", "category"])

    # Column checks
    missing_pred = required_pred - set(pred_df.columns)
    if missing_pred:
        print(f"⚠️ Predictions missing columns {missing_pred}; returning empty evaluation.")
        return pd.DataFrame(columns=["player_id", "date", "tier", f"actual_{stat_col}", "category"])

    if stat_col not in log_df.columns:
        print(f"⚠️ Game log missing '{stat_col}' column; returning empty evaluation.")
        return pd.DataFrame(columns=["player_id", "date", "tier", f"actual_{stat_col}", "category"])

    # Merge predictions with actuals
    df = pred_df[["player_id", "date", tier_col]].merge(
        log_df[["player_id", "date", stat_col]],
        on=["player_id", "date"],
        how="left",
    )

    # Keep only rows with actual values (drop games not yet played / not logged)
    df = df.dropna(subset=[stat_col])
    if df.empty:
        print("⚠️ No matching actuals for predictions; returning empty evaluation.")
        return pd.DataFrame(columns=["player_id", "date", "tier", f"actual_{stat_col}", "category"])

    # Safe numeric comparisons
    df[tier_col] = pd.to_numeric(df[tier_col], errors="coerce").fillna(-1).astype(int)
    df[stat_col] = pd.to_numeric(df[stat_col], errors="coerce").fillna(0).astype(int)

    # Categorize using vectorized conditions
    conditions = [
        (df[tier_col] >= high_tier) & (df[stat_col] == 0),
        (df[tier_col] <= low_tier)  & (df[stat_col] > 0),
        (df[tier_col] >= high_tier) & (df[stat_col] > 0),
        (df[tier_col] <= low_tier)  & (df[stat_col] == 0),
    ]
    categories = ["miss_high", "false_negative", "hit_high", "hit_low"]

    df["category"] = np.select(conditions, categories, default=None)
    out = df.loc[df["category"].notna(), ["player_id", "date", tier_col, stat_col, "category"]].copy()
    out = out.rename(columns={stat_col: f"actual_{stat_col}", tier_col: "tier"})

    # Return empty well-formed frame if nothing matched
    if out.empty:
        return pd.DataFrame(columns=["player_id", "date", "tier", f"actual_{stat_col}", "category"])

    return out


# ---------------- CLI (optional direct use) ----------------

if __name__ == "__main__":
    import argparse
    from analyzer.data_loader import load_game_log, load_structured_players
    from analyzer.feature_engineering import compute_rolling_stats, merge_context
    from analyzer.streaks import annotate_streaks
    from analyzer.ranking import assign_tiers

    p = argparse.ArgumentParser(description="Evaluate tier predictions against actual outcomes")
    p.add_argument("--archive", type=Path, required=True, help="Path to player game log JSONL")
    p.add_argument("--structured", type=Path, required=True, help="Path to structured players JSON")
    p.add_argument("--date", type=str, required=True, help="Pipeline date (YYYY-MM-DD)")
    p.add_argument("--output-dir", type=Path, default=Path("data/analysis"), help="Where to write outputs")
    p.add_argument("--stat-col", type=str, default="hits", help="Actual stat column to evaluate (default: hits)")
    p.add_argument("--high-tier", type=int, default=8, help="Threshold for high tier (default: 8)")
    p.add_argument("--low-tier", type=int, default=2, help="Threshold for low tier (default: 2)")
    args = p.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Load and build a ranked predictions frame
    log_df = load_game_log(args.archive)
    struct_df = load_structured_players(args.structured)
    feats = compute_rolling_stats(log_df)
    ctx = merge_context(feats, struct_df)
    streaked = annotate_streaks(ctx)
    ranked = assign_tiers(streaked)

    # Evaluate
    eval_df = evaluate_predictions(
        pred_df=ranked,
        log_df=log_df,
        tier_col="tier",
        stat_col=args.stat_col,
        high_tier=args.high_tier,
        low_tier=args.low_tier,
    )

    # Write outputs
    out_csv = args.output_dir / f"evaluation_{args.date}.csv"
    eval_df.to_csv(out_csv, index=False)
    print(f"💾 Wrote evaluation CSV to {out_csv}")

    # Also Parquet if available
    try:
        out_parq = args.output_dir / f"evaluation_{args.date}.parquet"
        eval_df.to_parquet(out_parq, index=False)
        print(f"💾 Wrote evaluation Parquet to {out_parq}")
    except Exception as e:
        print(f"⚠️ Skipped Parquet write (install pyarrow to enable): {e}")
