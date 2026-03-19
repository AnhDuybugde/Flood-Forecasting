#!/usr/bin/env python3
"""
build_mask_png.py — Pre-render flood mask PNGs for frontend overlay.

Replaces 400 DOM rectangles with 1 L.imageOverlay (PNG).
Frontend: O(1) render instead of O(n) rectangle creation.

Usage:
    python scripts/build_mask_png.py --region DaNang --date 2026-01-31
    python scripts/build_mask_png.py --region DaNang --all
"""

import argparse
import json
import os
import sys
import tempfile

import boto3
import numpy as np
import rasterio
from PIL import Image
from botocore.config import Config

R2_ACCOUNT_ID = os.getenv('R2_ACCOUNT_ID', '')
R2_ACCESS_KEY = os.getenv('R2_ACCESS_KEY_ID', '')
R2_SECRET_KEY = os.getenv('R2_SECRET_ACCESS_KEY', '')
BUCKET = os.getenv('R2_BUCKET_NAME', 'satellite-data-10x10')


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='auto',
    )


def build_flood_mask(s3, region, date):
    """Download flood label TIF, render RGBA PNG, upload to R2."""

    # Download label TIF
    r2_key = f'FloodData/{region}/LabelDaily/Flood_{date}.tif'

    tmp = tempfile.NamedTemporaryFile(suffix='.tif', delete=False)
    tmp_path = tmp.name
    tmp.close()

    try:
        try:
            s3.download_file(BUCKET, r2_key, tmp_path)
        except Exception as e:
            print(f'  ⚠️  Label TIF not found ({r2_key}) - ERROR: {e}')
            return False

        with rasterio.open(tmp_path) as src:
            data = src.read(1)
            rows, cols = data.shape
            nodata = src.nodata if src.nodata is not None else -9999

            # Create RGBA image
            img = Image.new('RGBA', (cols, rows), (0, 0, 0, 0))
            pixels = img.load()

            for r in range(rows):
                for c in range(cols):
                    v = data[r, c]
                    if v == nodata or np.isnan(v):
                        # Cloud/NoData → grey semi-transparent
                        pixels[c, r] = (128, 128, 128, 100)
                    elif v > 0:
                        # Flood → red
                        pixels[c, r] = (255, 50, 50, 140)
                    # else: no flood → fully transparent (default)

            # Save to temp PNG
            png_tmp = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
            png_path = png_tmp.name
            png_tmp.close()

            img.save(png_path, 'PNG')

            # Upload to R2
            out_key = f'FloodData/{region}/Mask/mask_{date}_label.png'
            with open(png_path, 'rb') as f:
                s3.put_object(
                    Bucket=BUCKET,
                    Key=out_key,
                    Body=f.read(),
                    ContentType='image/png',
                    CacheControl='public, max-age=86400',
                )

            size_bytes = os.path.getsize(png_path)
            os.unlink(png_path)
            print(f'  ✅ mask_{date}_label.png: {cols}x{rows}px, {size_bytes}B → {out_key}')
            return True
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def main():
    parser = argparse.ArgumentParser(description='Build flood mask PNGs for frontend overlay')
    parser.add_argument('--region', default='DaNang')
    parser.add_argument('--date', help='Specific date (YYYY-MM-DD)')
    parser.add_argument('--all', action='store_true', help='Process all dates from registry')
    args = parser.parse_args()

    if not R2_ACCOUNT_ID:
        print('❌ R2_ACCOUNT_ID not set')
        sys.exit(1)

    s3 = get_s3_client()

    if args.date:
        print(f'🎨 Building flood mask for {args.region} / {args.date}')
        build_flood_mask(s3, args.region, args.date)
    elif args.all:
        try:
            key = f'FloodData/{args.region}/metadata.json'
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            registry = json.loads(obj['Body'].read())
            dates = registry.get('dates', [])
            print(f'🎨 Building {len(dates)} flood masks for {args.region}')
            success = 0
            for date in dates:
                if build_flood_mask(s3, args.region, date):
                    success += 1
            print(f'\n✅ Built {success}/{len(dates)} masks')
        except Exception as e:
            print(f'❌ Cannot load registry: {e}')
            sys.exit(1)
    else:
        print('Specify --date YYYY-MM-DD or --all')
        sys.exit(1)


if __name__ == '__main__':
    main()
