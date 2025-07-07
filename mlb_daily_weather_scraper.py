import requests
import pandas as pd
from datetime import datetime

# === Config ===
target_date = datetime.now().strftime('%Y-%m-%d')
filename = f"mlb_weather_{target_date}.csv"  # Save directly in current directory

# Dome stadiums where weather doesn't matter
domes = {
    'Rogers Centre': 'Dome',
    'Tropicana Field': 'Dome',
    'Chase Field': 'Dome',
    'loanDepot Park': 'Dome',
    'Minute Maid Park': 'Dome',
    'Globe Life Field': 'Dome',
    'American Family Field': 'Dome',
    'T-Mobile Park': 'Dome'
}

# Stadium locations (lat, lon)
stadium_coords = {
    'Oriole Park at Camden Yards': (39.2839, -76.6218),
    'Fenway Park': (42.3467, -71.0972),
    'Yankee Stadium': (40.8296, -73.9262),
    'Rogers Centre': (43.6414, -79.3894),
    'Tropicana Field': (27.7683, -82.6534),
    'Progressive Field': (41.4962, -81.6852),
    'Comerica Park': (42.3390, -83.0485),
    'Kauffman Stadium': (39.0516, -94.4803),
    'Target Field': (44.9817, -93.2775),
    'Guaranteed Rate Field': (41.8300, -87.6339),
    'Globe Life Field': (32.7473, -97.0847),
    'Minute Maid Park': (29.7573, -95.3555),
    'Oakland Coliseum': (37.7516, -122.2005),
    'Angel Stadium': (33.8003, -117.8827),
    'T-Mobile Park': (47.5914, -122.3325),
    'Chase Field': (33.4455, -112.0667),
    'Coors Field': (39.7569, -104.9942),
    'Dodger Stadium': (34.0739, -118.2400),
    'loanDepot Park': (25.7781, -80.2197),
    'American Family Field': (43.0280, -87.9712),
    'Citi Field': (40.7571, -73.8458),
    'PNC Park': (40.4469, -80.0057),
    'Busch Stadium': (38.6226, -90.1928),
    'Petco Park': (32.7073, -117.1566),
    'Oracle Park': (37.7786, -122.3893),
    'Nationals Park': (38.8729, -77.0075),
    'Truist Park': (33.8908, -84.4678),
    'Wrigley Field': (41.9484, -87.6553),
    'Great American Ball Park': (39.0979, -84.5075),
    'Citizens Bank Park': (39.9057, -75.1665),
    'Sutter Health Park': (38.5802, -121.5137),
}

all_weather = []

# Step 1: Get Schedule
schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={target_date}"
schedule_resp = requests.get(schedule_url).json()
dates = schedule_resp.get('dates', [])

for date_info in dates:
    games = date_info.get('games', [])

    for game in games:
        game_time = game.get('gameDate', '')[11:16]
        teams = game.get('teams', {})
        home_team = teams.get('home', {}).get('team', {}).get('name', '')
        away_team = teams.get('away', {}).get('team', {}).get('name', '')
        venue = game.get('venue', {}).get('name', '')
        matchup = f"{away_team} @ {home_team}"

        if venue in domes:
            all_weather.append({
                'Matchup': matchup,
                'Venue': venue,
                'City': '',
                'Game Time (UTC)': game_time,
                'Temp (°F)': '',
                'Rain Chance (%)': '',
                'Wind (mph)': '',
                'Forecast': 'Dome—No Weather Impact'
            })
            continue

        coords = stadium_coords.get(venue)
        if coords:
            lat, lon = coords
            weather_url = f"https://api.weather.gov/points/{lat},{lon}"

            try:
                grid_resp = requests.get(weather_url, timeout=10).json()
                forecast_url = grid_resp['properties']['forecastHourly']
                forecast_resp = requests.get(forecast_url, timeout=10).json()
                periods = forecast_resp.get('properties', {}).get('periods', [])

                if periods:
                    first = periods[0]
                    temp = first.get('temperature', '')
                    wind = first.get('windSpeed', '')
                    forecast = first.get('shortForecast', '')
                    rain = first.get('probabilityOfPrecipitation', {}).get('value', '')
                    rain = rain if rain is not None else ''

                    all_weather.append({
                        'Matchup': matchup,
                        'Venue': venue,
                        'City': grid_resp.get('properties', {}).get('relativeLocation', {}).get('properties', {}).get('city', ''),
                        'Game Time (UTC)': game_time,
                        'Temp (°F)': temp,
                        'Rain Chance (%)': rain,
                        'Wind (mph)': wind,
                        'Forecast': forecast
                    })
                else:
                    all_weather.append({
                        'Matchup': matchup,
                        'Venue': venue,
                        'City': '',
                        'Game Time (UTC)': game_time,
                        'Temp (°F)': '',
                        'Rain Chance (%)': '',
                        'Wind (mph)': '',
                        'Forecast': 'No forecast available'
                    })

            except Exception as e:
                all_weather.append({
                    'Matchup': matchup,
                    'Venue': venue,
                    'City': '',
                    'Game Time (UTC)': game_time,
                    'Temp (°F)': '',
                    'Rain Chance (%)': '',
                    'Wind (mph)': '',
                    'Forecast': f'Error: {e}'
                })

        else:
            all_weather.append({
                'Matchup': matchup,
                'Venue': venue,
                'City': '',
                'Game Time (UTC)': game_time,
                'Temp (°F)': '',
                'Rain Chance (%)': '',
                'Wind (mph)': '',
                'Forecast': 'Unknown Stadium—No Data'
            })

# Save to CSV (no path, current folder)
df = pd.DataFrame(all_weather)
df.to_csv(filename, index=False)

print(f"\n✅ Done! Weather saved to {filename} with {len(df)} games.")

