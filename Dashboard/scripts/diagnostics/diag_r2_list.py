"""
Diagnostic: List actual R2 bucket structure.
Shows what keys exist under FloodData/ so we can compare with code expectations.
"""
import os, sys, json
from dotenv import load_dotenv
import boto3

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_BUCKET = os.environ.get('R2_BUCKET_NAME', 'satellite-data-10x10')

if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
    print("ERROR: R2 credentials not found in .env")
    sys.exit(1)

s3 = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto"
)

print(f"Bucket: {R2_BUCKET}")
print(f"Endpoint: https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com")
print("=" * 80)

# List top-level prefixes
print("\n--- TOP-LEVEL PREFIXES ---")
resp = s3.list_objects_v2(Bucket=R2_BUCKET, Delimiter='/', MaxKeys=100)
for p in resp.get('CommonPrefixes', []):
    print(f"  {p['Prefix']}")
for o in resp.get('Contents', []):
    print(f"  [file] {o['Key']}  ({o['Size']} bytes)")

# List FloodData/ structure
print("\n--- FloodData/ STRUCTURE (first 50 keys) ---")
resp = s3.list_objects_v2(Bucket=R2_BUCKET, Prefix='FloodData/', MaxKeys=50)
for o in resp.get('Contents', []):
    print(f"  {o['Key']}  ({o['Size']:,} bytes)")

# List FloodData/ subfolders
print("\n--- FloodData/ SUBFOLDERS ---")
resp = s3.list_objects_v2(Bucket=R2_BUCKET, Prefix='FloodData/', Delimiter='/')
for p in resp.get('CommonPrefixes', []):
    print(f"  {p['Prefix']}")

# Check DaNang specifically
for sub_prefix in ['FloodData/DaNang/', 'FloodData/danang/', 'FloodData/Da_Nang/', 'FloodData/DANANG/']:
    resp = s3.list_objects_v2(Bucket=R2_BUCKET, Prefix=sub_prefix, MaxKeys=5)
    count = resp.get('KeyCount', 0)
    if count > 0:
        print(f"\n--- {sub_prefix} (first 5 of {count}) ---")
        for o in resp.get('Contents', []):
            print(f"  {o['Key']}  ({o['Size']:,} bytes)")

# Also check common alternative structures
for prefix in ['DaNang/', 'danang/', 'stacked/', 'Stacked/', 'data/', 'Data/']:
    resp = s3.list_objects_v2(Bucket=R2_BUCKET, Prefix=prefix, MaxKeys=3)
    if resp.get('KeyCount', 0) > 0:
        print(f"\n--- {prefix} (found {resp['KeyCount']} keys) ---")
        for o in resp.get('Contents', []):
            print(f"  {o['Key']}  ({o['Size']:,} bytes)")

# Deep scan: find any .tif files in entire bucket
print("\n--- ALL .tif FILES (first 30) ---")
paginator = s3.get_paginator('list_objects_v2')
tif_count = 0
for page in paginator.paginate(Bucket=R2_BUCKET, MaxKeys=500):
    for o in page.get('Contents', []):
        if o['Key'].endswith('.tif'):
            print(f"  {o['Key']}  ({o['Size']:,} bytes)")
            tif_count += 1
            if tif_count >= 30:
                break
    if tif_count >= 30:
        break

if tif_count == 0:
    print("  (no .tif files found in bucket!)")

# Also look for metadata.json
print("\n--- metadata.json files ---")
for page in paginator.paginate(Bucket=R2_BUCKET, MaxKeys=500):
    for o in page.get('Contents', []):
        if 'metadata' in o['Key'].lower() and o['Key'].endswith('.json'):
            print(f"  {o['Key']}  ({o['Size']:,} bytes)")

print("\n--- TOTAL OBJECT COUNT ---")
resp = s3.list_objects_v2(Bucket=R2_BUCKET)
print(f"  Objects in bucket: {resp.get('KeyCount', 0)} (truncated: {resp.get('IsTruncated', False)})")
