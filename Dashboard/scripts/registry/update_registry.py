#!/usr/bin/env python3
"""
update_registry.py — Write-time metadata indexing for R2.

Runs AFTER upload pipeline to update metadata.json in R2.
Eliminates runtime R2 scans (ListObjectsV2 → 0 calls).

Usage:
    python scripts/update_registry.py --region DaNang --date 2026-01-31
    python scripts/update_registry.py --region DaNang --scan   # rebuild from R2
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime

import boto3
from botocore.config import Config

# ── R2 Config ───────────────────────────────────────────────
R2_ACCOUNT_ID = os.getenv('R2_ACCOUNT_ID', '')
R2_ACCESS_KEY = os.getenv('R2_ACCESS_KEY_ID', '')
R2_SECRET_KEY = os.getenv('R2_SECRET_ACCESS_KEY', '')
BUCKET = os.getenv('R2_BUCKET_NAME', 'satellite-data-10x10')

DAILY_LAYERS = ['rain', 'soilMoisture', 'tide', 'flood']
STATIC_LAYERS = ['dem', 'slope', 'flow', 'landCover']
BAND_ORDER = ['rain', 'soilMoisture', 'tide', 'flood', 'dem', 'slope', 'flow', 'landCover']


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='auto',
    )


def load_current_registry(s3, region):
    """Load existing metadata.json or create empty."""
    key = f'FloodData/{region}/metadata.json'
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3.exceptions.NoSuchKey:
        return {
            'version': 0,
            'region': region,
            'updatedAt': '',
            'checksum': '',
            'dateRange': {'start': '', 'end': ''},
            'totalDays': 0,
            'dates': [],
            'layers': {'daily': DAILY_LAYERS, 'static': STATIC_LAYERS},
            'stacked': {'available': [], 'bandOrder': BAND_ORDER},
        }
    except Exception as e:
        print(f'⚠️ Could not load registry: {e}')
        return None


def compute_checksum(dates):
    return hashlib.sha256(','.join(sorted(dates)).encode()).hexdigest()[:16]


def scan_r2_dates(s3, region):
    """Scan R2 for available dates (rebuild mode)."""
    prefix = f'FloodData/{region}/Daily/Rain/'
    dates = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            name = obj['Key'].split('/')[-1]
            # Rain_YYYY-MM-DD.tif → YYYY-MM-DD
            parts = name.replace('.tif', '').split('_')
            if len(parts) >= 2:
                dates.add(parts[1])
    return sorted(dates)


def scan_stacked_dates(s3, region):
    """Find which dates have stacked COGs."""
    prefix = f'FloodData/{region}/Stacked/'
    dates = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            name = obj['Key'].split('/')[-1]
            # stacked_YYYY-MM-DD.tif
            if name.startswith('stacked_') and name.endswith('.tif'):
                dates.append(name.replace('stacked_', '').replace('.tif', ''))
    return sorted(dates)


def update_registry(s3, region, new_date=None, scan=False):
    registry = load_current_registry(s3, region)
    if registry is None:
        print('❌ Cannot load registry, aborting')
        return

    if scan:
        print(f'🔍 Scanning R2 for dates in region {region}...')
        registry['dates'] = scan_r2_dates(s3, region)
        registry['stacked']['available'] = scan_stacked_dates(s3, region)
        print(f'   Found {len(registry["dates"])} dates, {len(registry["stacked"]["available"])} stacked')
    elif new_date:
        if new_date not in registry['dates']:
            registry['dates'].append(new_date)
            registry['dates'].sort()
            print(f'✅ Added date {new_date}')
        else:
            print(f'ℹ️  Date {new_date} already in registry')

    # Update metadata
    registry['version'] += 1
    registry['updatedAt'] = datetime.utcnow().isoformat() + 'Z'
    registry['totalDays'] = len(registry['dates'])
    registry['checksum'] = compute_checksum(registry['dates'])

    if registry['dates']:
        registry['dateRange'] = {
            'start': registry['dates'][0],
            'end': registry['dates'][-1],
        }

    # Upload
    key = f'FloodData/{region}/metadata.json'
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(registry, indent=2),
        ContentType='application/json',
    )
    print(f'📝 Uploaded {key} (v{registry["version"]}, {registry["totalDays"]} days)')


def main():
    parser = argparse.ArgumentParser(description='Update metadata registry in R2')
    parser.add_argument('--region', default='DaNang', help='Region name')
    parser.add_argument('--date', help='Add specific date (YYYY-MM-DD)')
    parser.add_argument('--scan', action='store_true', help='Rebuild by scanning R2')
    args = parser.parse_args()

    if not R2_ACCOUNT_ID:
        print('❌ R2_ACCOUNT_ID not set')
        sys.exit(1)

    s3 = get_s3_client()
    update_registry(s3, args.region, new_date=args.date, scan=args.scan)


if __name__ == '__main__':
    main()
