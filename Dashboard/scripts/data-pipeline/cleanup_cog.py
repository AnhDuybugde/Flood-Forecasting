"""
cleanup_cog.py — Delete all COG and Stacked files from R2.
"""
import os
import boto3
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'backend', '.env'))

R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_BUCKET = os.environ.get('R2_BUCKET_NAME', 'satellite-data-10x10')

s3 = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto"
)

PREFIXES_TO_DELETE = [
    'FloodData/DaNang/COG/',
    'FloodData/DaNang/Stacked/',
]

def delete_prefix(prefix):
    print(f"\n🗑️  Deleting all objects under: {prefix}")
    total = 0
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
        objects = page.get('Contents', [])
        if not objects:
            continue
        keys = [{'Key': obj['Key']} for obj in objects]
        s3.delete_objects(Bucket=R2_BUCKET, Delete={'Objects': keys, 'Quiet': True})
        total += len(keys)
        print(f"  Deleted batch: {len(keys)} (total: {total})")
    print(f"✅ Deleted {total} objects under {prefix}")
    return total

grand_total = 0
for prefix in PREFIXES_TO_DELETE:
    grand_total += delete_prefix(prefix)

print(f"\n✨ Done! Total deleted: {grand_total} objects")
