from __future__ import annotations

import json
from typing import Any, Iterable, Optional
import pandas as pd


def _to_dict(obj: Any) -> dict:
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str) and obj.strip():
        try:
            return json.loads(obj)
        except Exception:
            return {}
    return {}


def _first(d: dict, keys: Iterable[str], default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _to_float(x, default: float = None) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
        return float(x)
    except Exception:
        return default


def add_derived_context_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds robust, numeric features used by ranking:
      - ou_total: float (game total)
      - is_favorite: 1 if team appears favored (moneyline < 0 or < opponent)
      - temp_f, wind_mph, precip_prob: weather signals
      - is_switch_hitter: 1 if bats == 'S'
    Never raises; fills missing with neutral defaults.
    """
    if df.empty:
        return df

    out = df.copy()

    # ---- switch hitter
    bats_col = None
    for c in ("bats", "bat_hand", "batting_hand", "handedness_bat"):
        if c in out.columns:
            bats_col = c
            break
    out["is_switch_hitter"] = ((out[bats_col].astype(str).str.upper() == "S") if bats_col else 0).astype(int)

    # ---- betting_context extraction
    ou_vals, team_ml_vals, opp_ml_vals = [], [], []
    for _, row in out.iterrows():
        b = _to_dict(row.get("betting_context"))
        ou = _first(b, ["over_under", "total", "ou", "ou_total"], None)
        ou_vals.append(_to_float(ou))

        # Try to get team-specific moneyline if present, else generic 'moneyline'
        # We treat negative as favorite when opponent ML is unknown
        tm_ml = _first(b, ["team_moneyline", "moneyline", "ml"], None)

        # Look for an opponent moneyline if structure provides it
        # (Different scrapers may store as opponent_moneyline, opp_moneyline, etc.)
        op_ml = _first(b, ["opponent_moneyline", "opp_moneyline"], None)

        team_ml_vals.append(_to_float(tm_ml))
        opp_ml_vals.append(_to_float(op_ml))

    out["ou_total"] = pd.Series(ou_vals, index=out.index)
    out["team_moneyline"] = pd.Series(team_ml_vals, index=out.index)
    out["opp_moneyline"] = pd.Series(opp_ml_vals, index=out.index)

    def _is_fav(row) -> int:
        tm = row.get("team_moneyline")
        op = row.get("opp_moneyline")
        # If both are available, favored if tm < op (more negative)
        if pd.notna(tm) and pd.notna(op):
            return int(tm < op)
        # Fallback: negative ML implies favorite
        if pd.notna(tm):
            return int(tm < 0)
        return 0

    out["is_favorite"] = out.apply(_is_fav, axis=1).astype(int)

    # ---- weather_context extraction
    temps, winds, precips = [], [], []
    for _, row in out.iterrows():
        w = _to_dict(row.get("weather_context"))
        t = _first(w, ["temperature_f", "temp_f", "temperatureF", "temperature"], None)
        ws = _first(w, ["wind_mph", "windSpeed_mph", "wind_speed_mph", "wind_speed"], None)
        pp = _first(w, ["precip_probability", "precip_prob", "precip", "precipPercent"], None)
        temps.append(_to_float(t))
        winds.append(_to_float(ws))
        # normalize precip to [0,1] if it looks like percent
        p = _to_float(pp)
        if p is not None and p > 1:
            p = p / 100.0
        precips.append(p)

    out["temp_f"] = pd.Series(temps, index=out.index)
    out["wind_mph"] = pd.Series(winds, index=out.index)
    out["precip_prob"] = pd.Series(precips, index=out.index)

    # Fill neutral defaults
    out["ou_total"] = out["ou_total"].fillna(out["ou_total"].median() if out["ou_total"].notna().any() else 8.5)
    out["temp_f"] = out["temp_f"].fillna(out["temp_f"].median() if out["temp_f"].notna().any() else 70.0)
    out["wind_mph"] = out["wind_mph"].fillna(out["wind_mph"].median() if out["wind_mph"].notna().any() else 5.0)
    out["precip_prob"] = out["precip_prob"].fillna(0.0)
    out["is_favorite"] = out["is_favorite"].fillna(0).astype(int)

    return out
