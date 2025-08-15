import pandas as pd

# --- Fallback shim inserted by automation ---
def add_derived_context_features(df):
    """
    Fallback shim: keep pipeline running if the original implementation is missing.
    Adds common derived flags/columns with safe defaults so downstream steps don't crash.
    Replace with the full implementation when ready.
    """
    import pandas as pd
    out = df.copy()

    # Try to detect common batting-handedness column
    bats_col = None
    for c in ("bats","batting_hand","batter_hand","hitter_handedness"):
        if c in out.columns:
            bats_col = c
            break

    # Handedness flags (best effort)
    if bats_col:
        upper = out[bats_col].astype(str).str.upper()
        out["is_switch_hitter"] = upper.eq("S").astype(int)
        out["is_lefty_batter"]  = upper.eq("L").astype(int)
        out["is_righty_batter"] = upper.eq("R").astype(int)
    else:
        out["is_switch_hitter"] = 0
        out["is_lefty_batter"]  = 0
        out["is_righty_batter"] = 0

    # Weather context (neutral defaults if missing)
    for col, default in [("temp_f", None), ("wind_mph", None), ("precip_prob", None)]:
        if col not in out.columns:
            out[col] = default

    # Betting context (neutral/safe defaults if missing)
    for col, default in [("implied_total", None), ("is_favorite", None)]:
        if col not in out.columns:
            out[col] = default

    return out
# --- End fallback shim ---

