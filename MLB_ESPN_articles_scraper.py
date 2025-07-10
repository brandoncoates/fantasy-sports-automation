import feedparser
import pandas as pd
from datetime import datetime

# ESPN MLB News RSS Feed
rss_url = "http://www.espn.com/espn/rss/mlb/news"

# Parse RSS feed
feed = feedparser.parse(rss_url)

# Extract articles
articles = []
for entry in feed.entries:
    articles.append({
        "Title": entry.title,
        "Summary": entry.summary,
        "Link": entry.link,
        "Published": entry.published
    })

# Convert to DataFrame
df = pd.DataFrame(articles)

# Save as CSV with today's date
target_date = datetime.now().strftime('%Y-%m-%d')
filename = f"mlb_news_{target_date}.csv"
df.to_csv(filename, index=False)

print(f"✅ Saved {len(df)} news articles to {filename}")
