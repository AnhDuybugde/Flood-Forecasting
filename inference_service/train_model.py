"""
Train a flood prediction model from historical TIF data.

Reads training data from Cloudflare R2 (Stacked COG per date) or, fallback,
from local TIF files. No local data required when using R2.

Usage:
    python -m inference_service.train_model                    # Full training from R2
    python -m inference_service.train_model --sample 10000     # Subsample
    python -m inference_service.train_model --data-dir ./data  # (Deprecated) local TIF

Output:
    inference_service/model/flood_model.pkl
    inference_service/model/model_meta.json
"""
import os
import sys
import json
import glob
import argparse
import logging
import re
from datetime import datetime
from typing import List, Tuple, Optional

import numpy as np
import joblib

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("train")

FEATURE_NAMES = ["rainfall", "soilMoisture", "tide", "dem", "slope", "flow", "landCover"]
MODEL_DIR = os.path.join(os.path.dirname(__file__), "model")

# Stacked COG band order (matches merge_multiband.py and common.ts)
STACKED_BAND_SCALES = [1000, 1000, 1000, 1000, 1, 1, 1, 1]  # first 4 = daily, last 4 = static
NODATA = -9999


def _get_r2_client():
    """Lazy boto3 S3 client for R2. Loads env from project root or backend .env."""
    try:
        from dotenv import load_dotenv
        root = os.path.join(os.path.dirname(__file__), "..")
        load_dotenv(os.path.join(root, ".env"))
        load_dotenv(os.path.join(root, "backend", ".env"))
    except ImportError:
        pass
    account = os.environ.get("R2_ACCOUNT_ID")
    key_id = os.environ.get("R2_ACCESS_KEY_ID")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY")
    if not account or not key_id or not secret:
        return None
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name="auto",
    )


def build_dataset_from_r2(
    region: str = "DaNang",
    max_dates: int = 50,
    bucket: Optional[str] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build training dataset from R2 Stacked COG files. No local data used.

    - Fetches FloodData/{region}/metadata.json for list of dates (or lists Stacked prefix).
    - For each date, downloads FloodData/{region}/Stacked/stacked_YYYY-MM-DD.tif,
      reads 8 bands (rainfall, soilMoisture, tide, flood, dem, slope, flow, landCover),
      builds feature matrix X (7 bands excluding flood) and labels y (flood band > 0.5).
    - Falls back to generate_synthetic_dataset if R2 unavailable or no data.
    """
    s3 = _get_r2_client()
    if s3 is None:
        logger.warning("R2 credentials not set. Using synthetic data.")
        return generate_synthetic_dataset(5000)

    bucket = bucket or os.environ.get("R2_BUCKET_NAME", "satellite-data-10x10")
    prefix = f"FloodData/{region}/Stacked/"
    meta_key = f"FloodData/{region}/metadata.json"

    # Get list of dates: try metadata.json first
    dates: List[str] = []
    try:
        resp = s3.get_object(Bucket=bucket, Key=meta_key)
        meta = json.loads(resp["Body"].read().decode())
        if isinstance(meta.get("dates"), list):
            dates = sorted(meta["dates"])
        elif isinstance(meta.get("stacked"), dict) and isinstance(meta["stacked"].get("available"), list):
            dates = sorted(meta["stacked"]["available"])
    except Exception as e:
        logger.debug(f"Metadata not available: {e}")

    if not dates:
        # Fallback: list Stacked prefix
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    m = re.search(r"stacked_(\d{4}-\d{2}-\d{2})\.tif$", obj["Key"])
                    if m:
                        dates.append(m.group(1))
            dates = sorted(set(dates))
        except Exception as e:
            logger.warning(f"Could not list R2 Stacked: {e}")

    if not dates:
        logger.warning("No dates found on R2. Using synthetic data.")
        return generate_synthetic_dataset(5000)

    if len(dates) > max_dates:
        indices = np.linspace(0, len(dates) - 1, max_dates, dtype=int)
        dates = [dates[i] for i in indices]

    logger.info(f"Building dataset from R2: {len(dates)} dates")

    try:
        import rasterio
    except ImportError:
        logger.warning("rasterio not installed. Using synthetic data.")
        return generate_synthetic_dataset(5000)

    all_X = []
    all_y = []

    for date_str in dates:
        key = f"{prefix}stacked_{date_str}.tif"
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            buf = resp["Body"].read()
        except Exception as e:
            logger.debug(f"  Skip {key}: {e}")
            continue

        try:
            with rasterio.MemoryFile(buf) as mem:
                with mem.open() as src:
                    # Bands 1..8: Rain, SoilMoisture, Tide, Flood, DEM, Slope, Flow, LandCover
                    data = src.read()  # (8, H, W)
                    if data.shape[0] < 8:
                        continue
                    h, w = data.shape[1], data.shape[2]
                    # Scale: first 4 bands / 1000, last 4 / 1
                    scaled = np.zeros_like(data, dtype=np.float32)
                    for i in range(8):
                        raw = data[i].astype(np.float32)
                        raw = np.where(raw <= NODATA + 1, np.nan, raw)
                        scaled[i] = raw / STACKED_BAND_SCALES[i]
                    # Features: bands 0,1,2,4,5,6,7 (exclude flood band 3)
                    feature_bands = [0, 1, 2, 4, 5, 6, 7]
                    features = np.column_stack([scaled[i].ravel() for i in feature_bands])
                    labels = (scaled[3].ravel() > 0.5).astype(int)
                    # Mask nodata
                    valid = np.all(np.isfinite(features), axis=1)
                    features = features[valid]
                    labels = labels[valid]
                    if features.size == 0:
                        continue
                    all_X.append(features)
                    all_y.append(labels)
                    logger.info(f"  {date_str}: {len(labels)} valid pixels, {labels.sum()} flood")
        except Exception as e:
            logger.debug(f"  Skip {date_str}: {e}")
            continue

    if not all_X:
        logger.warning("No data loaded from R2. Using synthetic data.")
        return generate_synthetic_dataset(5000)

    X = np.vstack(all_X)
    y = np.concatenate(all_y)
    logger.info(f"Dataset: {X.shape[0]} samples, {y.sum()} flood ({100 * y.mean():.1f}%)")
    return X, y


def load_tif_as_array(tif_path: str) -> np.ndarray:
    """Load a GeoTIFF as a numpy array."""
    try:
        import rasterio
        with rasterio.open(tif_path) as src:
            return src.read(1).astype(np.float32)
    except ImportError:
        # Fallback: try tifffile
        import tifffile
        return tifffile.imread(tif_path).astype(np.float32)


def build_dataset_from_local(data_dir: str, max_dates: int = 50) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build training dataset from local TIF files.

    Expected structure:
      data_dir/
        Flood/Flood/Flood_YYYY-MM-DD.tif    (labels)
        Rain/Rain_YYYY-MM-DD.tif
        SoilMoisture/SoilMoisture_YYYY-MM-DD.tif
        Tide/Tide_YYYY-MM-DD.tif
        Static/DEM.tif
        Static/Slope.tif
        Static/Flow.tif
        Static/LandCover.tif

    Falls back to synthetic data if structure not found.
    """
    flood_dir = os.path.join(data_dir, "Flood", "Flood")
    flood_files = sorted(glob.glob(os.path.join(flood_dir, "Flood_*.tif")))

    if not flood_files:
        logger.warning(f"No flood TIF files found in {flood_dir}. Using synthetic data.")
        return generate_synthetic_dataset(5000)

    # Limit dates for training speed
    if len(flood_files) > max_dates:
        indices = np.linspace(0, len(flood_files) - 1, max_dates, dtype=int)
        flood_files = [flood_files[i] for i in indices]

    logger.info(f"Found {len(flood_files)} flood label files. Reading...")

    all_X = []
    all_y = []

    for flood_path in flood_files:
        # Extract date from filename
        basename = os.path.basename(flood_path)
        date_str = basename.replace("Flood_", "").replace(".tif", "")

        try:
            flood_arr = load_tif_as_array(flood_path)
        except Exception as e:
            logger.warning(f"  Skip {basename}: {e}")
            continue

        # Try to load corresponding daily layers
        rain_path = os.path.join(data_dir, "Rain", f"Rain_{date_str}.tif")
        sm_path = os.path.join(data_dir, "SoilMoisture", f"SoilMoisture_{date_str}.tif")
        tide_path = os.path.join(data_dir, "Tide", f"Tide_{date_str}.tif")

        # Static layers (same for all dates)
        dem_path = os.path.join(data_dir, "Static", "DEM.tif")
        slope_path = os.path.join(data_dir, "Static", "Slope.tif")
        flow_path = os.path.join(data_dir, "Static", "Flow.tif")
        lc_path = os.path.join(data_dir, "Static", "LandCover.tif")

        # Load available layers (use zeros for missing)
        h, w = flood_arr.shape
        rain = _safe_load(rain_path, h, w)
        sm = _safe_load(sm_path, h, w)
        tide = _safe_load(tide_path, h, w)
        dem = _safe_load(dem_path, h, w)
        slope = _safe_load(slope_path, h, w)
        flow = _safe_load(flow_path, h, w)
        lc = _safe_load(lc_path, h, w)

        # Flatten to pixel-level samples
        n_pixels = h * w
        features = np.column_stack([
            rain.ravel(),
            sm.ravel(),
            tide.ravel(),
            dem.ravel(),
            slope.ravel(),
            flow.ravel(),
            lc.ravel(),
        ])

        labels = (flood_arr.ravel() > 0.5).astype(int)  # Binary: flood=1, no_flood=0

        # Filter out nodata pixels (where all features are 0 or negative)
        valid_mask = np.any(features > -9998, axis=1) & (features[:, 0] > -9998)
        features = features[valid_mask]
        labels = labels[valid_mask]

        all_X.append(features)
        all_y.append(labels)

        logger.info(f"  {basename}: {len(labels)} valid pixels, {labels.sum()} flood")

    if not all_X:
        logger.warning("No data loaded. Using synthetic data.")
        return generate_synthetic_dataset(5000)

    X = np.vstack(all_X)
    y = np.concatenate(all_y)

    logger.info(f"Dataset: {X.shape[0]} samples, {y.sum()} flood ({100 * y.mean():.1f}%)")
    return X, y


def _safe_load(path: str, h: int, w: int) -> np.ndarray:
    """Load a TIF or return zeros if not found."""
    if os.path.exists(path):
        try:
            arr = load_tif_as_array(path)
            if arr.shape == (h, w):
                return arr
            # Resize if shape mismatch
            from scipy.ndimage import zoom
            return zoom(arr, (h / arr.shape[0], w / arr.shape[1]), order=1)
        except Exception:
            pass
    return np.zeros((h, w), dtype=np.float32)


def generate_synthetic_dataset(n_samples: int = 5000) -> Tuple[np.ndarray, np.ndarray]:
    """
    Generate synthetic training data based on domain knowledge.
    Used as fallback when real data is not available in expected structure.
    """
    logger.info(f"Generating {n_samples} synthetic samples...")
    rng = np.random.RandomState(42)

    # Features
    rainfall = rng.exponential(20, n_samples)          # mm
    soil_moisture = rng.uniform(10, 95, n_samples)     # %
    tide = rng.uniform(-0.5, 2.5, n_samples)           # m
    dem = rng.uniform(0, 100, n_samples)                # m
    slope = rng.exponential(5, n_samples)               # degrees
    flow = rng.exponential(200, n_samples)              # accumulation
    land_cover = rng.choice([1, 2, 3, 4, 5], n_samples)

    X = np.column_stack([rainfall, soil_moisture, tide, dem, slope, flow, land_cover])

    # Generate labels with realistic flood logic
    flood_score = (
        0.3 * np.clip(rainfall / 100, 0, 1)
        + 0.2 * np.clip(soil_moisture / 100, 0, 1)
        + 0.15 * np.clip(tide / 2.0, 0, 1)
        + 0.15 * np.clip(1.0 - dem / 50, 0, 1)
        + 0.1 * np.clip(1.0 - slope / 20, 0, 1)
        + 0.1 * np.clip(flow / 500, 0, 1)
    )
    # Add noise
    flood_score += rng.normal(0, 0.1, n_samples)
    y = (flood_score > 0.5).astype(int)

    logger.info(f"Synthetic data: {n_samples} samples, {y.sum()} flood ({100 * y.mean():.1f}%)")
    return X, y


def train_model(X: np.ndarray, y: np.ndarray, sample_limit: int = 0):
    """Train a Random Forest classifier and save to disk."""
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, f1_score, classification_report

    # Subsample if requested
    if sample_limit > 0 and X.shape[0] > sample_limit:
        indices = np.random.choice(X.shape[0], sample_limit, replace=False)
        X, y = X[indices], y[indices]
        logger.info(f"Subsampled to {sample_limit} samples")

    # Handle class imbalance
    if y.mean() < 0.05:
        logger.info("Severe class imbalance detected. Using class_weight='balanced'.")
        class_weight = "balanced"
    else:
        class_weight = None

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    logger.info(f"Training: {X_train.shape[0]} samples, Testing: {X_test.shape[0]} samples")

    # Train
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=15,
        min_samples_leaf=5,
        class_weight=class_weight,
        random_state=42,
        n_jobs=-1,
    )

    logger.info("Training Random Forest...")
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, zero_division=0)

    logger.info(f"\nAccuracy: {acc:.4f}")
    logger.info(f"F1 Score: {f1:.4f}")
    logger.info(f"\n{classification_report(y_test, y_pred, target_names=['No Flood', 'Flood'], zero_division=0)}")

    # Feature importance
    importances = model.feature_importances_
    for name, imp in sorted(zip(FEATURE_NAMES, importances), key=lambda x: -x[1]):
        logger.info(f"  Feature '{name}': {imp:.4f}")

    # Save model
    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = os.path.join(MODEL_DIR, "flood_model.pkl")
    joblib.dump(model, model_path)
    logger.info(f"\nModel saved: {model_path}")

    # Save metadata
    meta = {
        "version": "v1.0",
        "model_type": "RandomForestClassifier",
        "trained_at": datetime.now().isoformat(),
        "accuracy": round(acc, 4),
        "f1_score": round(f1, 4),
        "n_estimators": 100,
        "max_depth": 15,
        "train_samples": int(X_train.shape[0]),
        "test_samples": int(X_test.shape[0]),
        "feature_names": FEATURE_NAMES,
        "feature_importances": {
            name: round(float(imp), 4) for name, imp in zip(FEATURE_NAMES, importances)
        },
        "class_distribution": {
            "no_flood": int((y == 0).sum()),
            "flood": int((y == 1).sum()),
        },
    }
    meta_path = os.path.join(MODEL_DIR, "model_meta.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    logger.info(f"Metadata saved: {meta_path}")

    return model, meta


def main():
    parser = argparse.ArgumentParser(description="Train flood prediction model")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="(Deprecated) Local data directory; ignored when using R2. Use --source local to force.",
    )
    parser.add_argument(
        "--source",
        choices=("r2", "local"),
        default="r2",
        help="Data source: r2 (default) or local TIF (requires --data-dir).",
    )
    parser.add_argument("--sample", type=int, default=0, help="Max samples for training (0=all)")
    parser.add_argument("--max-dates", type=int, default=50, help="Max dates to process")
    parser.add_argument("--synthetic", action="store_true", help="Force synthetic data")
    parser.add_argument("--region", default="DaNang", help="Region for R2 (e.g. DaNang)")
    args = parser.parse_args()

    if args.synthetic:
        X, y = generate_synthetic_dataset(10000)
    elif args.source == "local" and args.data_dir:
        logger.info("Using local data (deprecated). Prefer --source r2.")
        X, y = build_dataset_from_local(args.data_dir, max_dates=args.max_dates)
    else:
        X, y = build_dataset_from_r2(region=args.region, max_dates=args.max_dates)

    train_model(X, y, sample_limit=args.sample)


if __name__ == "__main__":
    main()
