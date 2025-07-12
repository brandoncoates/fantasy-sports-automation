import os
import requests
import pandas as pd
from datetime import datetime

# ===== CONFIG =====
API_KEY = 'YOUR_API_KEY'  # <-- Replace with your OpenWeatherMap API key
output_dir = "MLB Daily Weather"
os.makedirs(output_dir, exist_ok=True)
date_str = datetime.now().strftime('%Y-%m-%d')
filename = f"mlb_weather_{date_str}.csv"
output_path = os.path.join(output_dir, filename)

# Stadium coordinates (city-based for simplicity)
stadiums = {
    "Angel Stadium": (33.8003, -117.8827),
    "Chase Field": (33.4455, -112.0667),
    "Truist Park": (33.8908, -84.4678),
    "Camden Yards": (39.2840, -76.6216),
    "Fenway Park": (42.3467, -71.0972),
    "Wrigley Field": (41.9484, -87.6553),
    "Great American Ball Park": (39.0972, -84.5078),
    "Progressive Field": (41.4962, -81.6852),
    "Coors Field": (39.7561, -104.9942),
    "Comerica Park": (42.3390, -83.0485),
    "Minute Maid Park": (29.7574, -95.3555),
    "Kauffman Stadium": (39.0517, -94.4803),
    "LoanDepot Park": (25.7781, -80.2195),
    "American Family Field": (43.0280, -87.9712),
    "Target Field": (44.9817, -93.2782),
    "Citi Field": (40.7571, -73.8458),
    "Yankee Stadium": (40.8296, -73.9262),
    "Oakland Coliseum": (37.7516, -122.2005),
    "Citizens Bank Park": (39.9061, -75.1665),
    "PNC Park": (40.4469, -80.0057),
    "Petco Park": (32.7073, -117.1566),
    "Oracle Park": (37.7786, -122.3893),
    "T-Mobile Park": (47.5914, -122.3325),
    "Busch Stadium": (38.6226, -90.1928),
    "Tropicana Field": (27.7683, -82.6534),
    "Globe Life Field": (32.7473, -97.0847),
    "Rogers Centre": (43.6414, -79.3894),
    "Nationals Park": (38.8728, -77.0074),
    "Guaranteed Rate Field": (41.8300, -87.6339)
}

# Step 1: Get today’s MLB schedule
schedule_url = f'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_str}'
response = requests.get(schedule_url)
schedule_data = response.json()

games = schedule_data.get('dates', [])[0].get('games', []) if schedule_data.get('dates') else []

weather_data = []

# Step 2: Loop through scheduled games
for game in games:
    venue = game.get('venue', {}).get('name', '')
    game_time = game.get('gameDate', '')

    lat_lon = stadiums.get(venue)

    if not lat_lon:
        print(f"⚠️ Skipped unknown venue: {venue}")
        continue

    lat, lon = lat_lon
    weather_url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=imperial'

    weather_response = requests.get(weather_url)
    if weather_response.status_code != 200:
        print(f"❌ Weather error for {venue}: {weather_response.status_code}")
        continue

    weather_json = weather_response.json()

    weather_data.append({
        'Venue': venue,
        'Game Time (UTC)': game_time,
        'Temperature (F)': weather_json.get('main', {}).get('temp'),
        'Feels Like (F)': weather_json.get('main', {}).get('feels_like'),
        'Weather': weather_json.get('weather', [{}])[0].get('description'),
        'Wind Speed (mph)': weather_json.get('wind', {}).get('speed'),
        'Humidity (%)': weather_json.get('main', {}).get('humidity')
    })

# Step 3: Save to CSV
df = pd.DataFrame(weather_data)
df.to_csv(output_path, index=False)

print(f"✅ Weather data saved to {output_path}")
