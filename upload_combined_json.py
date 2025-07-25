import boto3
from datetime import datetime

s3 = boto3.client("s3")
DATE = datetime.now().strftime("%Y-%m-%d")
filename = f"mlb_structured_players_{DATE}.json"

try:
    s3.upload_file(filename, "fantasy-sports-csvs", f"mlb/combined/{filename}")
    print(f"✅ Uploaded {filename} to s3://fantasy-sports-csvs/mlb/combined/")
except Exception as e:
    print(f"❌ Upload failed: {e}")
