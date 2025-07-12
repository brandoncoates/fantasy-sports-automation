import os
import boto3

# === Folder mapping: local folder (relative to repo root) ➜ S3 subfolder
folders = {
    "mlb_box_scores": "baseball/boxscores/",
    "mlb_daily_odds": "baseball/betting/",
    "mlb_daily_rosters": "baseball/rosters/",
    "mlb_daily_weather": "baseball/weather/",
    "mlb_espn_articles": "baseball/news/",
    "mlb_probable_starters": "baseball/starters/",
    "mlb_season_stats": "baseball/seasonstats/",
    "mlb_draftkings_player_props": "baseball/playerprojections/draftkings/",
    "mlb_draftkings_salaries": "baseball/salaries/draftkings/",
    "mlb_fanduel_player_props": "baseball/playerprojections/fanduel/",
    "mlb_fanduel_salaries": "baseball/salaries/fanduel/",
    "mlb_espn_player_projections": "baseball/playerprojections/espn/",
    "mlb_rotogrinders_projections": "baseball/playerprojections/rotogrinders/",
}

# === Get AWS region and connect to S3
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3 = boto3.client("s3", region_name=aws_region)
bucket_name = "fantasy-sports-csvs"

# === Handle GitHub Actions nested folder issue
cwd = os.getcwd()
if os.path.basename(cwd) == "fantasy-sports-automation":
    root_dir = cwd
else:
    root_dir = os.path.join(cwd, "fantasy-sports-automation")

print(f"🧭 Using root directory: {root_dir}")

# === Get the newest .csv file in a folder
def get_newest_csv_file(folder_path):
    csv_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".csv")
    ]
    if not csv_files:
        return None
    return max(csv_files, key=os.path.getmtime)

# === Upload each CSV
for local_folder, s3_folder in folders.items():
    folder_path = os.path.join(root_dir, local_folder)

    if not os.path.exists(folder_path):
        print(f"⚠️ Folder does not exist: {folder_path}")
        continue

    newest_file = get_newest_csv_file(folder_path)
    if not newest_file:
        print(f"⚠️ No CSV found in: {folder_path}")
        continue

    filename = os.path.basename(newest_file)
    s3_key = f"{s3_folder}{filename}"

    try:
        print(f"📤 Uploading {filename} → s3://{bucket_name}/{s3_key}")
        s3.upload_file(newest_file, bucket_name, s3_key)
        print(f"✅ Uploaded {filename}")
    except Exception as e:
        print(f"❌ Failed to upload {filename}: {e}")
