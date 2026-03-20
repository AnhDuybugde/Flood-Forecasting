#!/usr/bin/env python3
import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
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
MAX_WORKERS = 30

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version='s3v4', retries={'max_attempts': 10, 'mode': 'standard'}),
        region_name='auto',
    )

def main():
    parser = argparse.ArgumentParser(description="Replace Flood labels on R2 with local data.")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing them.")
    args = parser.parse_args()

    # Load S3 client
    if not R2_ACCOUNT_ID:
        print("❌ Lỗi: Không tìm thấy R2_ACCOUNT_ID trong .env")
        return

    s3 = get_s3_client()
    
    # 1. Get local files
    print(f"🔍 Quét thư mục local: {LOCAL_DIR}")
    if not os.path.exists(LOCAL_DIR):
        print(f"❌ Lỗi: Thư mục không tồn tại: {LOCAL_DIR}")
        return
        
    local_files = [f for f in os.listdir(LOCAL_DIR) if f.startswith("Flood_") and f.endswith(".tif")]
    print(f"✅ Tìm thấy {len(local_files)} files local.")

    # 2. Get R2 files
    print(f"🔍 Quét R2 bucket '{BUCKET}', prefix '{PREFIX}'")
    r2_objects = []
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX)
    try:
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.tif'):
                        r2_objects.append(obj)
    except Exception as e:
        print(f"❌ Lỗi khi lấy danh sách từ R2: {e}")
        return

    print(f"✅ Tìm thấy {len(r2_objects)} files trên R2.")

    # 3. Đối chiếu
    r2_keys = [obj['Key'] for obj in r2_objects]
    delete_keys = [{'Key': key} for key in r2_keys]
    
    if args.dry_run:
        print("\n[DRY RUN] Các file sẽ bị xóa trên R2:")
        print(f"Sẽ xóa {len(delete_keys)} files.")
        print("\n[DRY RUN] Các file sẽ được upload:")
        print(f"Sẽ upload {len(local_files)} files.")
    else:
        print(f"\n🗑️ Bắt đầu xóa {len(delete_keys)} files trên R2...")
        # Xóa batch (tối đa 1000 items mỗi lần)
        if len(delete_keys) > 0:
            for i in range(0, len(delete_keys), 1000):
                batch = delete_keys[i:i + 1000]
                try:
                    s3.delete_objects(Bucket=BUCKET, Delete={'Objects': batch})
                    print(f"  Đã xóa batch {i//1000 + 1}/{len(delete_keys)//1000 + 1} ({len(batch)} files)")
                except Exception as e:
                    print(f"❌ Lỗi xóa batch {i//1000 + 1}: {e}")
            print("✅ Xóa xong.")
        else:
            print("✅ Không có file nào trên R2 để xóa.")

        # 5. Upload files
        print(f"\n🚀 Bắt đầu upload {len(local_files)} files từ local lên R2 (Multi-thread {MAX_WORKERS})...")
        
        def upload_file(filename):
            local_path = os.path.join(LOCAL_DIR, filename)
            r2_key = f"{PREFIX}{filename}"
            try:
                s3.upload_file(local_path, BUCKET, r2_key)
                return True, filename
            except Exception as e:
                return False, f"{filename} - {e}"

        success_count = 0
        fail_count = 0
        
        if len(local_files) > 0:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(upload_file, f) for f in local_files]
                for idx, future in enumerate(as_completed(futures), 1):
                    success, msg = future.result()
                    if success:
                        success_count += 1
                    else:
                        fail_count += 1
                        print(f"❌ Lỗi upload: {msg}")
                    
                    if idx % 100 == 0 or idx == len(local_files):
                        print(f"  Tiến độ: {idx}/{len(local_files)} (Thành công: {success_count}, Thất bại: {fail_count})")
            
        print(f"\n✨ Xong! Đã upload thành công {success_count} files, thất bại {fail_count} files. ✨")

if __name__ == "__main__":
    main()
