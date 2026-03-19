"""
convert_to_cog.py — Convert GeoTIFF files in R2 to Cloud Optimized GeoTIFF (COG)

COG uses internal tiling (256x256 blocks) + DEFLATE compression.
This allows geotiff.js to use HTTP Range Requests to fetch ONLY the
tile containing the target pixel (~2-8KB) instead of the full file (~10-30MB).

Cold latency improvement: ~4000ms → ~200ms per file.

Usage:
    python scripts/convert_to_cog.py                    # Convert all dates
    python scripts/convert_to_cog.py --date 2020-01-08  # Convert one date
    python scripts/convert_to_cog.py --static-only      # Convert static layers only
"""

import os
import sys
import argparse
import tempfile
import logging
import boto3
import rasterio
from rasterio.transform import from_bounds
from dotenv import load_dotenv

# ── Config ──
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'backend', '.env'))

R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_BUCKET = os.environ.get('R2_BUCKET_NAME', 'satellite-data-10x10')

REGION = 'DaNang'

# Layers that exist per-date (daily)
DAILY_LAYERS = {
    'Rain':           f'FloodData/{REGION}/Daily/Rain',
    'SoilMoisture':   f'FloodData/{REGION}/Daily/SoilMoisture',
    'Tide':           f'FloodData/{REGION}/Daily/Tide',
    'Flood':          f'FloodData/{REGION}/LabelDaily',
}

# Static layers (no date suffix)
STATIC_LAYERS = {
    'DEM':       f'FloodData/{REGION}/Static/DEM.tif',
    'Slope':     f'FloodData/{REGION}/Static/Slope.tif',
    'Flow':      f'FloodData/{REGION}/Static/Flow.tif',
    'LandCover': f'FloodData/{REGION}/Static/LandCover.tif',
}

# COG output prefix
COG_PREFIX = f'FloodData/{REGION}/COG'

# ── Logging ──
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('COG')

# ── S3/R2 Client ──
s3 = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto"
)


def download_from_r2(r2_key, local_path):
    """Download a file from R2 to local path."""
    s3.download_file(R2_BUCKET, r2_key, local_path)


def upload_to_r2(local_path, r2_key):
    """Upload a local file to R2."""
    s3.upload_file(local_path, R2_BUCKET, r2_key)
    logger.info(f"  ☁️  Uploaded COG: {r2_key}")


def convert_single_tif_to_cog(r2_key_in, r2_key_out):
    """Download a TIF from R2, convert to COG, upload back."""
    # Check if COG already exists
    try:
        s3.head_object(Bucket=R2_BUCKET, Key=r2_key_out)
        logger.info(f"  ⏭️  Already exists: {r2_key_out}")
        return True
    except s3.exceptions.ClientError:
        pass

    with tempfile.TemporaryDirectory() as tmpdir:
        src_path = os.path.join(tmpdir, 'input.tif')
        dst_path = os.path.join(tmpdir, 'output_cog.tif')

        try:
            logger.info(f"  ⬇️  Downloading: {r2_key_in}")
            download_from_r2(r2_key_in, src_path)
        except Exception as e:
            logger.warning(f"  ❌ Download failed: {r2_key_in} → {e}")
            return False

        try:
            # Read source and write as COG
            with rasterio.open(src_path) as src:
                profile = src.profile.copy()
                data = src.read()

                # COG profile: tiled, DEFLATE compression, 256x256 blocks
                profile.update({
                    'driver': 'GTiff',
                    'compress': 'deflate',
                    'tiled': True,
                    'blockxsize': 256,
                    'blockysize': 256,
                    'predictor': 2,
                })

                # Write COG with overviews
                with rasterio.open(dst_path, 'w', **profile) as dst:
                    dst.write(data)

            logger.info(f"  ✅ Converted to COG: {os.path.getsize(dst_path)} bytes")
            upload_to_r2(dst_path, r2_key_out)
            return True
        except Exception as e:
            logger.error(f"  ❌ Conversion failed: {e}")
            return False


def list_daily_files(prefix):
    """List all TIF files under a prefix in R2."""
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.tif'):
                keys.append(obj['Key'])
    return keys


def convert_static_layers():
    """Convert all static layers to COG."""
    logger.info("🏛️  Converting static layers...")
    for name, r2_key in STATIC_LAYERS.items():
        out_key = f"{COG_PREFIX}/Static/{name}.tif"
        convert_single_tif_to_cog(r2_key, out_key)


def convert_daily_layers(target_date=None):
    """Convert daily layers to COG."""
    for layer_name, prefix in DAILY_LAYERS.items():
        logger.info(f"📅 Processing layer: {layer_name}")
        files = list_daily_files(prefix)
        
        for r2_key in files:
            # Extract date from filename (e.g., Rain_2020-01-08.tif)
            basename = os.path.basename(r2_key)
            
            if target_date and target_date not in basename:
                continue
            
            out_key = f"{COG_PREFIX}/Daily/{layer_name}/{basename}"
            convert_single_tif_to_cog(r2_key, out_key)


def main():
    parser = argparse.ArgumentParser(description='Convert GeoTIFF to COG')
    parser.add_argument('--date', type=str, help='Convert only this date (YYYY-MM-DD)')
    parser.add_argument('--static-only', action='store_true', help='Convert static layers only')
    args = parser.parse_args()

    logger.info("🚀 COG Conversion Pipeline Starting...")
    
    convert_static_layers()
    
    if not args.static_only:
        convert_daily_layers(target_date=args.date)
    
    logger.info("✨ COG Conversion Complete!")


if __name__ == '__main__':
    main()
