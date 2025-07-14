import os
import csv
import time
import boto3
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Direct URLs to props
PROP_URLS = {
    "Hits": "https://sportsbook.draftkings.com/event/mlb-player-hits",
    "Home Runs": "https://sportsbook.draftkings.com/event/mlb-player-home-runs",
    "RBIs": "https://sportsbook.draftkings.com/event/mlb-player-runs-batted-in",
    "Pitcher Strikeouts": "https://sportsbook.draftkings.com/event/mlb-player-strikeouts"
}

def get_driver():
    options = uc.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    return uc.Chrome(options=options)

def scrape_props():
    driver = get_driver()
    wait = WebDriverWait(driver, 20)
    all_data = []

    for prop_type, url in PROP_URLS.items():
        print(f"🔗 Visiting {prop_type} page")
        driver.get(url)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(5)

        # Save screenshot for this prop
        screenshot_path = f"/tmp/dk_{prop_type.replace(' ', '_').lower()}.png"
        driver.save_screenshot(screenshot_path)
        print(f"📸 Screenshot for {prop_type} saved to: {screenshot_path}")

        rows = driver.find_elements(By.XPATH, "//div[contains(@class,'sportsbook-event-accordion__wrapper')]")
        print(f"📦 Found {len(rows)} rows for {prop_type}")

        for row in rows:
            try:
                player = row.find_element(By.CLASS_NAME, "event-cell__name-text").text
                line = row.find_element(By.CLASS_NAME, "sportsbook-outcome-cell__line").text
                outcomes = row.find_elements(By.CLASS_NAME, "sportsbook-outcome-cell__element")
                over_odds = outcomes[0].text.split("\n")[-1]
                under_odds = outcomes[1].text.split("\n")[-1]
                all_data.append([player, prop_type, line, over_odds, under_odds])
            except Exception:
                continue

        time.sleep(2)

    driver.quit()
    return all_data

def save_to_csv(data, filename):
    headers = ["Player", "Prop Type", "Line", "Over Odds", "Under Odds"]
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

def upload_to_s3(local_path, bucket_name, s3_path):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    s3.upload_file(local_path, bucket_name, s3_path)

def main():
    data = scrape_props()
    print(f"✅ Total props scraped: {len(data)}")

    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"draftkings_props_{today}.csv"
    local_path = f"/tmp/{filename}"
    s3_path = f"baseball/props-dk/{filename}"
    bucket = "fantasy-sports-csvs"

    save_to_csv(data, local_path)
    upload_to_s3(local_path, bucket, s3_path)
    print(f"✅ Uploaded to S3: {s3_path}")

if __name__ == "__main__":
    main()
