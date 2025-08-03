#!/usr/bin/env python3
import os
import shutil
import json
import boto3
import subprocess
from datetime import datetime

# CONFIG
BUCKET = "fantasy-sports-csvs"
REGION = "us-east-2"
LOCAL_DIR = os.getcwd()
ARCHIVE_DIR = os.path.join(LOCAL_DIR, "baseball", "combined", "archive")
DATE = datetime.now().strftime("%Y-%m-%d")

# Filenames
raw_filename = f"structured_players_{DATE}.json"
enhanced_filename = f"enhanced_structured_players_{DATE}.json"

# Filepaths
raw_fp = os.path.join(LOCAL_DIR, raw_filename)
archive_fp = os.path.join(ARCHIVE_DIR, raw_filename)

# Step 1: Run combine script (optional, skip if already done)
# subprocess.run(["python", "combine.py"], check=True)

# Step 2: Move raw file to archive
print(f"📦 Archiving {raw_filename}...")
os.makedirs(ARCHIVE_DIR, exist_ok=True)
if os.path.exists(raw_fp):
    shutil.copy2(raw_fp, archive_fp)
else:
    raise FileNotFoundError(f"{raw_fp} not found!")

# Step 3: Generate enhanced file
print("🔍 Running player_stats_analyzer.py...")
subprocess.run(["python", "player_stats_analyzer.py"], check=True)

# Step 4: Upload both files to S3
s3 = boto3.client("s3", region_name=REGION)

def upload_to_s3(local_path, s3_key):
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"{local_path} does not exist.")
    print(f"☁️ Uploading {local_path} to s3://{BUCKET}/{s3_key}")
    s3.upload_file(local_path, BUCKET, s3_key)

upload_to_s3(archive_fp, f"baseball/combined/archive/{raw_filename}")
upload_to_s3(os.path.join(LOCAL_DIR, enhanced_filename), f"baseball/combined/{enhanced_filename}")

print("✅ All files uploaded successfully.")
