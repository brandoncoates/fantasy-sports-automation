import os
import boto3

# Define folder mappings: {local folder name : s3 subfolder}
folders = {
    "MLB Box Scores": "baseball/boxscores/",
    "MLB Daily Odds": "baseball/betting/",
    "MLB Daily Rosters": "baseball/rosters/",
    "MLB Daily Weather": "baseball/weather/",
    "MLB ESPN Articles": "baseball/news/",
    "MLB Probable Starters": "baseball/starters/",
    "MLB Season Stats": "baseball/seasonstats/"
}

# Set up S3 client (uses GitHub Action's configured AWS credentials)
s3 = boto3.client('s3')
bucket_name = 'fantasy-sports-csvs'

def get_newest_csv_file(folder_path):
    csv_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".csv")
    ]
    if not csv_files:
        return None
    return max(csv_files, key=os.path.getmtime)

# Upload newest file from each folder
for local_folder, s3_folder in folders.items():
    local_folder_path = os.path.join(os.getcwd(), local_folder)  # Relative to repo root
    if not os.path.exists(local_folder_path):
        print(f"⚠️ Folder does not exist: {local_folder_path}")
        continue

    newest_file = get_newest_csv_file(local_folder_path)

    if newest_file:
        filename = os.path.basename(newest_file)
        s3_key = f"{s3_folder}{filename}"

        try:
            s3.upload_file(newest_file, bucket_name, s3_key)
            print(f"✅ Uploaded: {filename} to s3://{bucket_name}/{s3_key}")
        except Exception as e:
            print(f"❌ Failed to upload {filename}: {e}")
    else:
        print(f"⚠️ No CSV files found in: {local_folder_path}")
