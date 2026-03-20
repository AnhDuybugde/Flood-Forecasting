#!/usr/bin/env python3
"""
Retry upload missing Flood labels to R2.
- Low concurrency (3 threads) to avoid SSL EOF errors
- Per-file retry with exponential backoff (max 5 attempts)
- Disables multipart upload (put_object for small files)
"""
import os
import time
import boto3
from botocore.config import Config
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

sys.stdout.reconfigure(encoding='utf-8')

# Load env variables from root DAP folder
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

R2_ACCOUNT_ID = os.getenv('R2_ACCOUNT_ID')
R2_ACCESS_KEY = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
BUCKET = os.getenv('R2_BUCKET_NAME', 'satellite-data-10x10')
LOCAL_DIR = r"C:\Users\ttung05\Desktop\DAP\data\Flood\Flood"
PREFIX = "FloodData/DaNang/LabelDaily/"
MAX_WORKERS = 3          # Reduced from 12 to avoid SSL overload
MAX_RETRIES = 5           # Per-file retries
RETRY_BASE_DELAY = 2      # seconds


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(
            signature_version='s3v4',
            retries={'max_attempts': 3, 'mode': 'standard'},
            max_pool_connections=5,
        ),
        region_name='auto',
    )


def upload_with_retry(s3, filename):
    """Upload a single file with exponential backoff retry."""
    local_path = os.path.join(LOCAL_DIR, filename)
    r2_key = f"{PREFIX}{filename}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Use put_object instead of upload_file to skip multipart
            with open(local_path, 'rb') as f:
                s3.put_object(Bucket=BUCKET, Key=r2_key, Body=f.read())
            return True, filename
        except Exception as e:
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                time.sleep(delay)
            else:
                return False, f"{filename} (after {MAX_RETRIES} attempts) - {e}"

    return False, f"{filename} - exhausted retries"


def main():
    if not R2_ACCOUNT_ID:
        print("❌ Lỗi: Không tìm thấy R2_ACCOUNT_ID trong .env")
        return

    s3 = get_s3_client()

    # 1. Get local files
    print(f"🔍 Quét thư mục local: {LOCAL_DIR}")
    local_files = [f for f in os.listdir(LOCAL_DIR) if f.startswith("Flood_") and f.endswith(".tif")]
    print(f"✅ Tìm thấy {len(local_files)} files local.")

    # 2. Get R2 files
    print(f"🔍 Quét R2 bucket '{BUCKET}', prefix '{PREFIX}'")
    r2_objects = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.tif'):
                r2_objects.add(obj['Key'].split('/')[-1])

    print(f"✅ Tìm thấy {len(r2_objects)} files trên R2.")

    # 3. Find missing
    missing_files = sorted([f for f in local_files if f not in r2_objects])
    print(f"🔍 Còn thiếu {len(missing_files)} file chưa được upload thành công.")

    if len(missing_files) == 0:
        print("🎉 Toàn bộ dữ liệu đã có trên R2. Không cần upload thêm.")
        return

    # 4. Upload missing files with low concurrency + retry
    print(f"\n🚀 Upload {len(missing_files)} files (threads={MAX_WORKERS}, retries={MAX_RETRIES})...")

    success_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(upload_with_retry, s3, f): f for f in missing_files}
        for idx, future in enumerate(as_completed(futures), 1):
            success, msg = future.result()
            if success:
                success_count += 1
                print(f"  [{idx}/{len(missing_files)}] ✅ {msg}")
            else:
                fail_count += 1
                print(f"  [{idx}/{len(missing_files)}] ❌ {msg}")

    print(f"\n✨ Xong! Thành công: {success_count}, thất bại: {fail_count} ✨")


if __name__ == "__main__":
    main()
