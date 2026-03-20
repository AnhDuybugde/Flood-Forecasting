import ee
import geemap
import os
import sys
import pandas as pd
import logging
import copernicusmarine as cm
import rasterio
import numpy as np
from dotenv import load_dotenv
from rasterio.warp import reproject, Resampling

# --- 📝 CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FloodCrawlMaster")

# --- 📂 CẤU HÌNH ĐƯỜNG DẪN & TÀI KHOẢN ---
load_dotenv()
BASE_DIR = "/opt/airflow/Data/data_original"
USER = os.getenv("COPERNICUS_USER") 
PW = os.getenv("COPERNICUS_PASS")

# --- 🗺️ CẤU HÌNH VÙNG CHỌN (Hằng số cơ bản) ---
DANANG_BBOX = [107.90, 15.95, 108.40, 16.25]

# --- 🔑 KHỞI TẠO EARTH ENGINE ---
def init_ee():
    try:
        key_path = "/opt/airflow/config/gee-key.json"
        if os.path.exists(key_path):
            credentials = ee.ServiceAccountCredentials('', key_path)
            ee.Initialize(credentials)
            logger.info("🔐 GEE initialized via JSON Key file")
        else:
            ee.Initialize()
            logger.info("🔐 GEE initialized via Default Credentials")
    except Exception as e:
        logger.error(f"❌ GEE Auth Failed: {e}")
        sys.exit(1)

# --- 🛠️ HÀM TẢI LOCAL (Cần truyền ROI vào) ---
def download_now(image, folder, name, scale, roi):
    path = os.path.join(BASE_DIR, folder, f"{name}.tif")
    if not os.path.exists(path):
        try:
            geemap.download_ee_image(image, filename=path, scale=scale, region=roi, crs="EPSG:4326")
            logger.info(f"💾 Saved: {name}")
            return True
        except Exception as e:
            logger.error(f"❌ Error downloading {name}: {e}")
            return False
    else:
        return True

def run_crawl(target_date):
    # BƯỚC 1: KHỞI TẠO EE TRƯỚC KHI GỌI BẤT KỲ HÀM EE NÀO
    init_ee()
    
    # BƯỚC 2: SAU KHI INIT MỚI ĐƯỢC TẠO GEOMETRY
    roi = ee.Geometry.Rectangle(DANANG_BBOX)
    
    # Đảm bảo cấu trúc thư mục
    for f in ["Static", "Daily/Rain", "Daily/Soil", "Daily/Tide", "Daily/FloodLabel"]: 
        os.makedirs(os.path.join(BASE_DIR, f), exist_ok=True)

    curr = ee.Date(target_date)
    
    # ----------------------------------------------------------
    # 🏛️ 1. STATIC LAYERS (Chỉ tải nếu chưa có)
    # ----------------------------------------------------------
    logger.info("🏛️ Checking Static Layers...")
    dem = ee.Image("USGS/SRTMGL1_003").clip(roi)
    download_now(dem, "Static", "Terrain_DEM_Raw", 30, roi)
    download_now(ee.Terrain.slope(dem), "Static", "Terrain_Slope_Raw", 30, roi)
    download_now(ee.Image("WWF/HydroSHEDS/15ACC").clip(roi), "Static", "Terrain_Flow_Raw", 30, roi)
    download_now(ee.ImageCollection("ESA/WorldCover/v200").first().clip(roi), "Static", "LandCover_ESA_Raw", 10, roi)

    geom_mask = dem.mask()

    # ----------------------------------------------------------
    # 📅 2. DAILY LAYERS
    # ----------------------------------------------------------
    logger.info(f"📅 --- PROCESSING DATE: {target_date} ---")

    # 🌧️ Rain (CHIRPS)
    rain = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").filterDate(curr, curr.advance(1, 'day')).sum().clip(roi)
    download_now(rain.updateMask(geom_mask), "Daily/Rain", f"Rain_{target_date}", 5566, roi)

    # 🌱 Soil Moisture (SMAP)
    soil_col = ee.ImageCollection("NASA/SMAP/SPL4SMGP/008") \
                     .filterDate(curr.advance(-1, 'day'), curr.advance(1, 'day')) \
                     .select('sm_surface')
    if soil_col.size().getInfo() > 0:
        download_now(soil_col.mean().clip(roi).updateMask(geom_mask), "Daily/Soil", f"Soil_{target_date}", 9000, roi)
    else:
        logger.warning(f"⚠️ No Soil data for {target_date}")

    # ⚓ Tide (Copernicus)
    tide_path = os.path.join(BASE_DIR, "Daily/Tide", f"Tide_{target_date}.tif")
    if not os.path.exists(tide_path):
        try:
            dem_local = os.path.join(BASE_DIR, "Static/Terrain_DEM_Raw.tif")
            if os.path.exists(dem_local):
                with rasterio.open(dem_local) as src:
                    m_meta = src.meta.copy()
                    m_shape = (src.height, src.width)
                    m_transform = src.transform

                ds = cm.open_dataset(
                    dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
                    variables=["zos"],
                    minimum_longitude=DANANG_BBOX[0], maximum_longitude=DANANG_BBOX[2],
                    minimum_latitude=DANANG_BBOX[1], maximum_latitude=DANANG_BBOX[3],
                    start_datetime=target_date, end_datetime=target_date,
                    username=USER, password=PW
                )
                
                data = ds['zos'].sel(time=target_date, method="nearest").values
                if len(data.shape) == 3: data = data[0]

                tide_aligned = np.zeros(m_shape, dtype=np.float32)
                raw_h, raw_w = data.shape
                raw_transform = rasterio.transform.from_origin(
                    DANANG_BBOX[0], DANANG_BBOX[3], 
                    (DANANG_BBOX[2]-DANANG_BBOX[0])/raw_w, (DANANG_BBOX[3]-DANANG_BBOX[1])/raw_h
                )

                reproject(
                    source=data.astype(np.float32), destination=tide_aligned,
                    src_transform=raw_transform, src_crs="EPSG:4326",
                    dst_transform=m_transform, dst_crs="EPSG:4326",
                    resampling=Resampling.bilinear
                )

                m_meta.update({"dtype": "float32", "count": 1, "nodata": np.nan})
                with rasterio.open(tide_path, "w", **m_meta) as dst:
                    dst.write(tide_aligned, 1)
                ds.close()
                logger.info(f"⚓ Tide Aligned: {target_date}")
        except Exception as e:
            logger.error(f"⚓ Tide Fail at {target_date}: {e}")

    # 🌊 Flood Label (Sentinel-1 SAR)
    s1_day = ee.ImageCollection('COPERNICUS/S1_GRD') \
                   .filterBounds(roi) \
                   .filterDate(curr, curr.advance(1, 'day')) \
                   .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))

    if s1_day.size().getInfo() > 0:
        flood_img = s1_day.mosaic().clip(roi).updateMask(geom_mask)
        download_now(flood_img, "Daily/FloodLabel", f"Flood_SAR_{target_date}", 10, roi)
    else:
        logger.warning(f"⚠️ No SAR data for {target_date}")

# --- 🚀 ENTRY POINT ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_ds = sys.argv[1]
        run_crawl(target_ds)
        logger.info(f"✨ HOÀN THÀNH CÀO DỮ LIỆU NGÀY {target_ds}")
    else:
        logger.error("❌ Cần truyền tham số ngày YYYY-MM-DD")
        sys.exit(1)