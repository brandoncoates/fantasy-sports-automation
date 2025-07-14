import os
import time
import csv
import boto3
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Only scrape these props
TARGET_PROPS = ["Hits", "Home Runs", "RBIs", "Pitcher Strikeouts"]

# DraftKings MLB Props URL (this may vary based on region/session)
DK_URL = "https://sportsbook.draftkings.com/leagues/baseball/mlb"

def get_driver():
    options = uc.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    return uc.Chrome(options=options)

def scrape_props():
    driver = get_driver()
    driver.get(DK_URL)

    wait = WebDriverWait(driver, 20)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(5)  # Allow extra time for JS load

    data = []

    for prop_type in TARGET_PROPS:
        print(f"Looking for prop: {prop_type}")

        try:
            # Try to click into the prop category if it exists
            prop_buttons = driver.find_elements(By.XPATH, f"//span[contains(text(), '{prop_type}')]")
            if not prop_buttons:
                print(f"{prop_type} not found.")
                continue

            prop_buttons[0].click()
            time.sleep(4)

            # Extract markets
            rows = driver.find_elements(By.XPATH, "//div[contains(@class,'sportsbook-event-accordion__wrapper')]")
            for row in rows:
                try:
                    player = row.find_element(By.CLASS_NAME, "event-cell__name-text").text
                    line = row.find_element(By.CLASS_NAME, "sportsbook-outcome-cell__line").text
                    outcomes = row.find_elements(By.CLASS_NAME, "sportsbook-outcome-cell__element")
                    over_odds = outcomes[0].text.split("\n")[-1]
                    under_odds = outcomes[1].text.split("\n")[-1]
                    data.append([player, prop_type, line, over_odds, under_odds])
                except Exception:
                    continue

        except Exception as e:
            print(f"Error scraping {prop_type}: {str(e)}")

        driver.back()
        time.sleep(3)

    driver.quit()
    return data

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
    print(f"Total props scraped: {len(data)}")

    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"draftkings_props_{today}.csv"
    local_path = f"/tmp/{filename}"
    s3_path = f"baseball/props-dk/{filename}"
    bucket = "fantasy-sports-csvs"

    save_to_csv(data, local_path)
    upload_to_s3(local_path, bucket, s3_path)
    print(f"Uploaded to S3: {s3_path}")

if __name__ == "__main__":
    main()
