"""
Diagnostic: Inspect local NPZ file to understand exact structure and values.
This shows what a pixel API should return.
"""
import numpy as np
import os, sys

data_dir = os.path.join(os.path.dirname(__file__), '..', 'data', '2020-2025', 'Data_Training_Soft_NPZ')
sample = os.path.join(data_dir, 'Sample_2020-01-03.npz')

if not os.path.exists(sample):
    print(f"ERROR: {sample} not found")
    sys.exit(1)

d = np.load(sample)
print(f"File: {sample}")
print(f"Keys: {list(d.keys())}")

for k in d.keys():
    arr = d[k]
    print(f"\n  {k}: shape={arr.shape}, dtype={arr.dtype}")
    print(f"    min={arr.min():.4f}, max={arr.max():.4f}, mean={arr.mean():.4f}")
    print(f"    nan_count={np.isnan(arr).sum()}, zeros={np.sum(arr == 0)}")

x = d['x']  # (8, H, W)
y = d['y']  # (H, W)

print(f"\n{'='*60}")
print(f"x shape: {x.shape}")
print(f"y shape: {y.shape}")
num_bands, H, W = x.shape
print(f"Grid: {H} rows x {W} cols = {H*W} pixels")

# Band names (assumed from STACKED_BAND_NAMES in common.ts)
band_names = ['rainfall', 'soilMoisture', 'tide', 'flood', 'dem', 'slope', 'flow', 'landCover']
print(f"\nBand-by-band stats:")
for i, name in enumerate(band_names):
    band = x[i]
    valid = band[~np.isnan(band)]
    nodata = np.sum((band <= -9998) | np.isnan(band))
    print(f"  Band {i} ({name:15s}): min={valid.min():.4f}  max={valid.max():.4f}  mean={valid.mean():.4f}  nodata={nodata}")

# Simulate pixel lookup at center of DaNang region
# DaNang: north=16.25, south=15.95, east=108.40, west=107.90
north, south, east, west = 16.25, 15.95, 108.40, 107.90

# Pick a test point: center of DaNang
test_lat, test_lng = 16.10, 108.15
col = int((test_lng - west) / (east - west) * W)
row = int((north - test_lat) / (north - south) * H)
col = max(0, min(col, W - 1))
row = max(0, min(row, H - 1))

print(f"\n{'='*60}")
print(f"Simulated pixel lookup at ({test_lat}, {test_lng})")
print(f"  -> row={row}, col={col}")
print(f"  Band values:")
for i, name in enumerate(band_names):
    val = x[i, row, col]
    print(f"    {name:15s} = {val:.4f}")
print(f"  Label (flood) = {y[row, col]:.4f}")

# Check a few more pixels to see if data is non-trivial
print(f"\n{'='*60}")
print(f"Sample pixels from corners and center:")
for label, r, c in [("top-left", 0, 0), ("top-right", 0, W-1), ("center", H//2, W//2), ("bottom-left", H-1, 0), ("bottom-right", H-1, W-1)]:
    vals = [x[i, r, c] for i in range(num_bands)]
    lab = y[r, c]
    print(f"  {label:15s} [{r},{c}]: bands={[f'{v:.2f}' for v in vals]}, label={lab:.2f}")

# Check scaling: the merge_multiband.py uses scale 1000 for first 4 bands, 1 for last 4
print(f"\n{'='*60}")
print(f"Checking if data needs de-scaling (first 4 bands /1000, last 4 /1):")
for i, name in enumerate(band_names):
    band = x[i]
    valid = band[(~np.isnan(band)) & (band > -9998)]
    if len(valid) > 0:
        is_scaled = valid.max() > 100
        print(f"  Band {i} ({name:15s}): max={valid.max():.1f}  -> {'LIKELY SCALED (divide by 1000)' if is_scaled and i < 4 else 'RAW VALUES'}")
