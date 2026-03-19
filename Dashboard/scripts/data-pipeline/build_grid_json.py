#!/usr/bin/env python3
"""
build_grid_json.py — Pre-build grid JSON files for zero-decode runtime.

Reads TIF rasters and outputs compact JSON grids to R2.
Runtime: GET grid.json → return immediately (no raster decode).

Usage:
    python scripts/build_grid_json.py --region DaNang --date 2026-01-31
    python scripts/build_grid_json.py --region DaNang --all
"""

import argparse
import json
import os
import sys
import tempfile

import boto3
import numpy as np
import rasterio
from botocore.config import Config

R2_ACCOUNT_ID = os.getenv('R2_ACCOUNT_ID', '')
R2_ACCESS_KEY = os.getenv('R2_ACCESS_KEY_ID', '')
R2_SECRET_KEY = os.getenv('R2_SECRET_ACCESS_KEY', '')
BUCKET = os.getenv('R2_BUCKET_NAME', 'satellite-data-10x10')

LAYERS = {
    'rain':         {'sub': 'Daily', 'folder': 'Rain', 'prefix': 'Rain', 'scale': 1000},
    'soilMoisture': {'sub': 'Daily', 'folder': 'SoilMoisture', 'prefix': 'SoilMoisture', 'scale': 1000},
    'tide':         {'sub': 'Daily', 'folder': 'Tide', 'prefix': 'Tide', 'scale': 1000},
    'label':        {'sub': 'LabelDaily', 'folder': '', 'prefix': 'Flood', 'scale': 1000},
}


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='auto',
    )


def build_grid_json(s3, region, date, layer_name, layer_info):
    """Download TIF, extract grid, upload JSON."""
    # Build R2 key
    if layer_info.get('folder'):
        r2_key = f"FloodData/{region}/{layer_info['sub']}/{layer_info['folder']}/{layer_info['prefix']}_{date}.tif"
    else:
        r2_key = f"FloodData/{region}/{layer_info['sub']}/{layer_info['prefix']}_{date}.tif"

    # Download TIF to temp
    tmp = tempfile.NamedTemporaryFile(suffix='.tif', delete=False)
    tmp_path = tmp.name
    tmp.close()
    
    try:
        try:
            s3.download_file(BUCKET, r2_key, tmp_path)
        except Exception as e:
            print(f'  ⚠️  {layer_name}: TIF not found ({r2_key}) - ERROR: {e}')
            return False

        with rasterio.open(tmp_path) as src:
            data = src.read(1)
            bounds = src.bounds
            rows, cols = data.shape
            nodata = src.nodata if src.nodata is not None else -9999

            flat = []
            for r in range(rows):
                for c in range(cols):
                    v = data[r, c]
                    if v == nodata or np.isnan(v):
                        flat.append(-9999)
                    else:
                        flat.append(int(round(v)))

            grid = {
                'v': 1,
                'region': region,
                'date': date,
                'layer': layer_name,
                'bounds': {
                    'n': round(bounds.top, 6),
                    's': round(bounds.bottom, 6),
                    'e': round(bounds.right, 6),
                    'w': round(bounds.left, 6),
                },
                'size': {'r': rows, 'c': cols},
                'scale': layer_info['scale'],
                'nodata': -9999,
                'data': flat,
            }

            # Upload JSON to R2
            out_key = f'FloodData/{region}/Grid/grid_{date}_{layer_name}.json'
            s3.put_object(
                Bucket=BUCKET,
                Key=out_key,
                Body=json.dumps(grid, separators=(',', ':')),
                ContentType='application/json',
            )
            size_kb = len(json.dumps(grid)) / 1024
            print(f'  ✅ {layer_name}: {rows}x{cols} → {out_key} ({size_kb:.1f}KB)')
            return True
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def process_date(s3, region, date):
    print(f'\n📊 Building grids for {region} / {date}')
    count = 0
    for layer_name, layer_info in LAYERS.items():
        if build_grid_json(s3, region, date, layer_name, layer_info):
            count += 1
    print(f'   Built {count}/{len(LAYERS)} grid JSONs')


def main():
    parser = argparse.ArgumentParser(description='Build pre-computed grid JSON files')
    parser.add_argument('--region', default='DaNang')
    parser.add_argument('--date', help='Specific date (YYYY-MM-DD)')
    parser.add_argument('--all', action='store_true', help='Process all dates from registry')
    args = parser.parse_args()

    if not R2_ACCOUNT_ID:
        print('❌ R2_ACCOUNT_ID not set')
        sys.exit(1)

    s3 = get_s3_client()

    if args.date:
        process_date(s3, args.region, args.date)
    elif args.all:
        # Load dates from registry
        try:
            key = f'FloodData/{args.region}/metadata.json'
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            registry = json.loads(obj['Body'].read())
            for date in registry.get('dates', []):
                process_date(s3, args.region, date)
        except Exception as e:
            print(f'❌ Cannot load registry: {e}')
            sys.exit(1)
    else:
        print('Specify --date YYYY-MM-DD or --all')
        sys.exit(1)


if __name__ == '__main__':
    main()
