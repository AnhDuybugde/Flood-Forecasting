#!/usr/bin/env python3
"""Quick check: compare local Flood files vs R2 uploads."""
import os, boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

s3 = boto3.client('s3',
    endpoint_url=f"https://{os.getenv('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
    aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
    config=Config(signature_version='s3v4'), region_name='auto')

BUCKET = os.getenv('R2_BUCKET_NAME', 'satellite-data-10x10')
PREFIX = 'FloodData/DaNang/LabelDaily/'
LOCAL_DIR = r'C:\Users\ttung05\Desktop\DAP\data\Flood\Flood'

# Count R2
r2 = set()
for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=PREFIX):
    for obj in page.get('Contents', []):
        if obj['Key'].endswith('.tif'):
            r2.add(obj['Key'].split('/')[-1])

# Count local
local = [f for f in os.listdir(LOCAL_DIR) if f.startswith('Flood_') and f.endswith('.tif')]
miss = sorted([f for f in local if f not in r2])

print(f"=== FLOOD LABEL UPLOAD PROGRESS ===")
print(f"Local files:  {len(local)}")
print(f"R2 files:     {len(r2)}")
print(f"Missing:      {len(miss)}")
print(f"Progress:     {len(r2)}/{len(local)} ({100*len(r2)/len(local):.1f}%)")

if miss:
    print(f"\nMissing range: {miss[0]} -> {miss[-1]}")
    print(f"Sample: {miss[:5]}")
else:
    print("\n*** ALL FILES UPLOADED SUCCESSFULLY ***")
