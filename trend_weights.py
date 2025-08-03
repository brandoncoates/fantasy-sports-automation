# trend_weights.py

# You can import this module in your DFS article generator to apply weighted logic
# to recent player trends using weather, matchup strength, and home/away context.

def apply_weighting_to_trends(player, trends):
    """
    Applies contextual weights to a player's recent average stats.

    Args:
        player (dict): Structured player entry with weather, matchup, and betting context.
        trends (dict): Recent average stats for the player.

    Returns:
        dict: Weighted trends dictionary.
    """
    weight = 1.0

    # Weather boost for good conditions
    weather = player.get("weather_context", {}).get("weather", {})
    temp = weather.get("temperature_f")
    wind = weather.get("wind_speed_mph")
    cloud = player.get("weather_context", {}).get("cloud_cover_pct")

    if temp and 65 <= temp <= 85:
        weight += 0.1
    if wind and wind > 10:
        weight += 0.05
    if cloud is not None and cloud < 40:
        weight += 0.05

    # Home advantage
    if player.get("home_or_away") == "home":
        weight += 0.05

    # Matchup boost for facing weaker opponents
    opp = player.get("opponent_team")
    implied_totals = player.get("betting_context", {}).get("implied_totals", {})
    team_total = implied_totals.get(player.get("team"))
    opp_total = implied_totals.get(opp)

    if opp_total is not None and team_total is not None:
        if team_total > opp_total:
            weight += 0.1

    # Cap weight to prevent extreme boosts
    weight = min(weight, 1.3)

    # Apply weight to each stat
    return {k: round(v * weight, 2) for k, v in trends.items()}


def apply_weighted_trends_to_all(players):
    for name, player in players.items():
        if "recent_averages" in player:
            weighted = apply_weighting_to_trends(player, player["recent_averages"])
            player["weighted_trends"] = weighted
    return players
