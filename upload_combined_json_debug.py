import boto3
import os
from datetime import datetime

s3 = boto3.client("s3")
DATE = "2025-07-25"
filename = f"mlb_structured_players_{DATE}.json"

if not os.path.exists(filename) or os.path.getsize(filename) == 0:
    print("❌ File is missing or empty. Upload aborted.")
    exit()

try:
    s3.upload_file(filename, "fantasy-sports-csvs", f"mlb/combined/{filename}")
    print(f"✅ Uploaded {filename} to s3://fantasy-sports-csvs/mlb/combined/")
except Exception as e:
    print(f"❌ Upload failed: {e}")
