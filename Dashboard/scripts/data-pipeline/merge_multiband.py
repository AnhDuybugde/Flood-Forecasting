"""
merge_multiband.py — Merge 8 separate TIF layers into single multi-band COG per date.

Performance gain:
    Before: 8 HTTP calls × ~50ms TTFB = ~400ms network latency
    After:  1 HTTP call  × ~50ms TTFB = ~50ms  (8× faster)

Band mapping:
    Band 1: Rain            (scale 1000)
    Band 2: SoilMoisture    (scale 1000)
    Band 3: Tide            (scale 1000)
    Band 4: Flood Label     (scale 1000)
    Band 5: DEM             (scale 1)
    Band 6: Slope           (scale 1)
    Band 7: Flow            (scale 1)
    Band 8: LandCover       (scale 1)

Usage:
    python scripts/merge_multiband.py                    # Merge all dates
    python scripts/merge_multiband.py --date 2020-01-08  # Merge one date
"""

import os
import sys
import argparse
import tempfile
import logging
import re
import numpy as np
import boto3
import rasterio
from rasterio.warp import reproject, Resampling
from dotenv import load_dotenv

# ── Config ──
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'backend', '.env'))

R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_BUCKET = os.environ.get('R2_BUCKET_NAME', 'satellite-data-10x10')

REGION = 'DaNang'

# Band order (must match api.js LAYER_FOLDER_MAP order)
BAND_CONFIG = [
    {'name': 'Rain',          'key_template': f'FloodData/{REGION}/Daily/Rain/Rain_{{date}}.tif'},
    {'name': 'SoilMoisture',  'key_template': f'FloodData/{REGION}/Daily/SoilMoisture/SoilMoisture_{{date}}.tif'},
    {'name': 'Tide',          'key_template': f'FloodData/{REGION}/Daily/Tide/Tide_{{date}}.tif'},
    {'name': 'Flood',         'key_template': f'FloodData/{REGION}/LabelDaily/Flood_{{date}}.tif'},
    {'name': 'DEM',           'key_template': f'FloodData/{REGION}/Static/DEM.tif',       'is_static': True},
    {'name': 'Slope',         'key_template': f'FloodData/{REGION}/Static/Slope.tif',     'is_static': True},
    {'name': 'Flow',          'key_template': f'FloodData/{REGION}/Static/Flow.tif',      'is_static': True},
    {'name': 'LandCover',     'key_template': f'FloodData/{REGION}/Static/LandCover.tif', 'is_static': True},
]

STACKED_PREFIX = f'FloodData/{REGION}/Stacked'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('Multiband')

s3 = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto"
)


def r2_exists(key):
    try:
        s3.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except:
        return False


def download_tif(r2_key, local_path):
    """Download TIF from R2, return True if success."""
    try:
        s3.download_file(R2_BUCKET, r2_key, local_path)
        return True
    except Exception as e:
        logger.warning(f"  ⚠️  Missing: {r2_key}")
        return False


def get_available_dates():
    """Scan Rain folder to find all available dates."""
    prefix = f'FloodData/{REGION}/Daily/Rain/'
    dates = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            m = re.search(r'(\d{4}-\d{2}-\d{2})\.tif$', obj['Key'])
            if m:
                dates.add(m.group(1))
    return sorted(dates)


def merge_date(date, tmpdir, static_cache):
    """Merge all 8 layers for a single date into one multi-band COG."""
    out_key = f'{STACKED_PREFIX}/stacked_{date}.tif'
    
    # Skip if already exists
    if r2_exists(out_key):
        logger.info(f"  ⏭️  Already merged: {out_key}")
        return True

    logger.info(f"📅 Merging date: {date}")
    
    # Reference raster (Rain) for dimensions/transform
    ref_path = os.path.join(tmpdir, f'ref_{date}.tif')
    ref_key = BAND_CONFIG[0]['key_template'].format(date=date)
    if not download_tif(ref_key, ref_path):
        logger.warning(f"  ❌ No Rain data for {date}, skipping")
        return False
    
    with rasterio.open(ref_path) as ref_src:
        ref_meta = ref_src.meta.copy()
        ref_height = ref_src.height
        ref_width = ref_src.width
        ref_transform = ref_src.transform
        ref_crs = ref_src.crs

    # Build 8-band array
    stacked = np.full((8, ref_height, ref_width), -9999, dtype=np.int16)

    for band_idx, band_conf in enumerate(BAND_CONFIG):
        is_static = band_conf.get('is_static', False)
        
        if is_static:
            # Use cached static data
            if band_conf['name'] in static_cache:
                stacked[band_idx] = static_cache[band_conf['name']]
                continue
        
        # Download layer
        r2_key = band_conf['key_template'].format(date=date) if not is_static else band_conf['key_template']
        local_path = os.path.join(tmpdir, f"band_{band_idx}_{date}.tif")
        
        if download_tif(r2_key, local_path):
            with rasterio.open(local_path) as src:
                data = src.read(1)
                
                # Reproject if shape doesn't match reference
                if data.shape != (ref_height, ref_width):
                    dest = np.full((ref_height, ref_width), -9999, dtype=np.int16)
                    reproject(
                        source=data, destination=dest,
                        src_transform=src.transform, src_crs=src.crs,
                        dst_transform=ref_transform, dst_crs=ref_crs,
                        resampling=Resampling.nearest
                    )
                    data = dest
                
                stacked[band_idx] = data.astype(np.int16)
                
                # Cache static layers
                if is_static:
                    static_cache[band_conf['name']] = stacked[band_idx].copy()
            
            # Clean up
            os.remove(local_path)
        else:
            logger.warning(f"  ⚠️  Band {band_idx} ({band_conf['name']}) missing, filled with nodata")

    # Write multi-band COG
    out_path = os.path.join(tmpdir, f'stacked_{date}.tif')
    profile = ref_meta.copy()
    profile.update({
        'count': 8,
        'dtype': 'int16',
        'driver': 'GTiff',
        'compress': 'deflate',
        'tiled': True,
        'blockxsize': 256,
        'blockysize': 256,
        'predictor': 2,
        'nodata': -9999,
    })

    with rasterio.open(out_path, 'w', **profile) as dst:
        for i in range(8):
            dst.write(stacked[i], i + 1)
            dst.set_band_description(i + 1, BAND_CONFIG[i]['name'])

    file_size = os.path.getsize(out_path) / (1024 * 1024)
    logger.info(f"  ✅ Stacked COG: {file_size:.1f} MB → {out_key}")
    
    # Upload
    s3.upload_file(out_path, R2_BUCKET, out_key)
    os.remove(out_path)
    
    # Clean ref
    if os.path.exists(ref_path):
        os.remove(ref_path)
    
    return True


def main():
    parser = argparse.ArgumentParser(description='Merge 8 layers into multi-band COG')
    parser.add_argument('--date', type=str, help='Merge only this date (YYYY-MM-DD)')
    args = parser.parse_args()

    logger.info("🚀 Multi-band Merge Pipeline Starting...")

    if args.date:
        dates = [args.date]
    else:
        logger.info("📋 Scanning available dates...")
        dates = get_available_dates()
        logger.info(f"   Found {len(dates)} dates")

    static_cache = {}  # Cache static layers across dates

    with tempfile.TemporaryDirectory() as tmpdir:
        for i, date in enumerate(dates):
            logger.info(f"[{i+1}/{len(dates)}] Processing {date}")
            merge_date(date, tmpdir, static_cache)

    logger.info("✨ Multi-band Merge Complete!")


if __name__ == '__main__':
    main()
