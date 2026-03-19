import os
import copernicusmarine as cm
import xarray as xr
import pandas as pd
import rioxarray
import numpy as np
from shapely.geometry import box

from config import USERNAME, PASSWORD


# =====================
# HELPER
# =====================
def unit_scale(x, vmin, vmax):
    return (x - vmin) / (vmax - vmin)


# =====================
# REGIONS
# =====================
REGIONS = {
    # "DBSCL": {
    #     "bbox": (104.4, 8.5, 106.8, 11.0)
    # },
    "CentralCoast": {
        "bbox": (107.4, 13.5, 109.5, 16.5)
    }
}

# =====================
# DATE RANGE
# =====================
START_DATE = "2000-02-1"
END_DATE   = "2025-12-31"


# =====================
# MAIN LOOP (PER REGION)
# =====================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

for region_name, cfg in REGIONS.items():
    print(f"\n🚀 Processing region: {region_name}")

    min_lon, min_lat, max_lon, max_lat = cfg["bbox"]
    region_geom = box(min_lon, min_lat, max_lon, max_lat)

    # ----- OUTPUT PATH (DIRECT, NO EXTRA FOLDER) -----
    BASE_DIR = os.path.join(SCRIPT_DIR, "FloodData", region_name, "Daily")

    os.makedirs(BASE_DIR, exist_ok=True)

    # =====================
    # LOAD COPERNICUS DATA
    # =====================
    ds = cm.open_dataset(
        dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
        username=USERNAME,
        password=PASSWORD,
        variables=["zos"],
        minimum_longitude=min_lon,
        maximum_longitude=max_lon,
        minimum_latitude=min_lat,
        maximum_latitude=max_lat,
        start_datetime=START_DATE,
        end_datetime=END_DATE
    )

    # Fix CRS
    ds = ds.rio.write_crs("EPSG:4326")

    # =====================
    # DAILY LOOP
    # =====================
    for t in ds.time.values:
        day = pd.to_datetime(t).strftime("%Y_%m_%d")

        # ---------- SELECT + CLIP ----------
        img = (
            ds["zos"]
            .sel(time=t)
            .rio.clip([region_geom], crs="EPSG:4326")
            .astype("float32")
        )

        # ---------- REPROJECT (≈500m, NO RESIZE) ----------
        img_500m = img.rio.reproject(
            dst_crs="EPSG:4326",
            resolution=0.005
        )

        # ---------- NORMALIZE (GEE-LIKE unitScale) ----------
        img_norm = unit_scale(
            img_500m.clip(-1.5, 1.5),
            -1.5,
            1.5
        ).clip(0, 1)

        img_norm = img_norm.assign_attrs(
            units="normalized",
            description="Daily sea level anomaly (zos), Copernicus, GEE-like unitScale"
        )

        # ---------- EXPORT ----------
        out_path = f"{BASE_DIR}/Tide_{day}.tif"
        img_norm.rio.to_raster(out_path, compress="LZW")

        print(f"✅ Saved Tide {region_name} {day}")

print("\n✨ DONE: Copernicus DAILY Tide for all regions")
