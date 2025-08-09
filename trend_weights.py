# trend_weights.py
"""
Contextual weighting for recent player trends.

This module is OPTIONAL. It is not required for the analyzer; it’s meant for
downstream scoring / DFS write-ups to bias recent averages using:
- weather_context (temperature_f, wind_speed_mph, roof_status)
- home_or_away
- betting_context.implied_totals (if present)

It is designed to be tolerant of missing fields and only multiplies numeric stats.
"""

from typing import Dict, Any, Mapping, Optional


# ---- Tunable constants -----------------------------------------------------

TEMP_GOOD_MIN = 65      # °F
TEMP_GOOD_MAX = 85
TEMP_BONUS    = 0.10

WIND_BOOST_THRESHOLD = 10.0  # mph
WIND_BONUS           = 0.05

HOME_BONUS           = 0.05
FAVORITE_BONUS       = 0.10

MAX_WEIGHT           = 1.30  # cap the total multiplier


# ---- Helpers ---------------------------------------------------------------

def _num(x) -> Optional[float]:
    """Return float(x) if x looks numeric, else None."""
    try:
        return float(x)
    except Exception:
        return None


def _is_numeric(x) -> bool:
    try:
        float(x)
        return True
    except Exception:
        return False


# ---- Core weighting --------------------------------------------------------

def apply_weighting_to_trends(player: Mapping[str, Any],
                              trends: Mapping[str, Any]) -> Dict[str, float]:
    """
    Apply a bounded multiplicative weight to a player's recent averages.

    Args:
        player: structured player dict from structured_players_<date>.json.
                Expected keys used here:
                  - "weather_context": dict with
                        temperature_f, wind_speed_mph, roof_status  (optional)
                  - "home_or_away": "home" | "away" (optional)
                  - "team": canonical team name (optional)
                  - "opponent_team": canonical team name (optional)
                  - "betting_context": dict with "implied_totals" (optional)
        trends: dict of recent averages, e.g. {"hits": 1.2, "home_runs": 0.3, ...}

    Returns:
        New dict of weighted trends (numeric keys scaled, others skipped).
    """
    weight = 1.0

    # --- Weather weighting (inner dict already in combine as weather_context)
    wctx = player.get("weather_context") or {}
    temp = _num(wctx.get("temperature_f"))
    wind = _num(wctx.get("wind_speed_mph"))
    roof = (wctx.get("roof_status") or "").lower()

    # Comfortable temps
    if temp is not None and TEMP_GOOD_MIN <= temp <= TEMP_GOOD_MAX:
        weight += TEMP_BONUS

    # Wind bonus only matters if roof isn't closed
    if (roof != "closed") and (wind is not None) and (wind > WIND_BOOST_THRESHOLD):
        weight += WIND_BONUS

    # --- Home advantage
    if (player.get("home_or_away") or "").lower() == "home":
        weight += HOME_BONUS

    # --- Favorite vs underdog via implied totals (if present)
    team = player.get("team")
    opp  = player.get("opponent_team")
    implied_totals = (player.get("betting_context") or {}).get("implied_totals") or {}
    if isinstance(implied_totals, dict) and team in implied_totals and opp in implied_totals:
        team_total = _num(implied_totals.get(team))
        opp_total  = _num(implied_totals.get(opp))
        if team_total is not None and opp_total is not None and team_total > opp_total:
            weight += FAVORITE_BONUS

    # --- Cap the total weight to avoid runaway boosts
    if weight > MAX_WEIGHT:
        weight = MAX_WEIGHT

    # --- Apply to numeric trend values only
    out: Dict[str, float] = {}
    for k, v in trends.items():
        if _is_numeric(v):
            out[k] = round(float(v) * weight, 3)
        # If non-numeric, we skip it on purpose (keeps output purely numeric)

    return out


def apply_weighted_trends_to_all(players: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    For each player entry (as in structured_players JSON), if it has a
    'recent_averages' dict, compute 'weighted_trends' using the logic above.
    Mutates and returns the same players dict for convenience.
    """
    for name, player in players.items():
        trends = player.get("recent_averages")
        if isinstance(trends, dict) and trends:
            player["weighted_trends"] = apply_weighting_to_trends(player, trends)
    return players
