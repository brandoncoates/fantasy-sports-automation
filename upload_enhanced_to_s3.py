#!/usr/bin/env python3
import os
import json
from datetime import datetime
import subprocess
import boto3

# CONFIG
BUCKET = "fantasy-sports-csvs"
S3_FOLDER = "baseball/combined"
LOCAL_FOLDER = os.getcwd()  # Or set this manually if needed
DATE = datetime.now().strftime("%Y-%m-%d")
FILENAME = f"enhanced_structured_players_{DATE}.json"
LOCAL_FILEPATH = os.path.join(LOCAL_FOLDER, FILENAME)

# 1. Run the analyzer
print("📊 Running player_stats_analyzer.py...")
subprocess.run(["python", "player_stats_analyzer.py"], check=True)

# 2. Check if the output file exists
if not os.path.exists(LOCAL_FILEPATH):
    raise FileNotFoundError(f"{FILENAME} not found after running analyzer.")

# 3. Upload to S3
print(f"☁️ Uploading {FILENAME} to S3...")

s3 = boto3.client("s3", region_name="us-east-2")
s3.upload_file(LOCAL_FILEPATH, BUCKET, f"{S3_FOLDER}/{FILENAME}")

print(f"✅ Upload complete: s3://{BUCKET}/{S3_FOLDER}/{FILENAME}")
