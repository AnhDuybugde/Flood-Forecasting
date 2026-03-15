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
import json

# --- 📝 CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FloodLocalMaster")

# --- 📂 CẤU HÌNH ĐƯỜNG DẪN & TÀI KHOẢN ---
load_dotenv() # Docker sẽ nạp .env vào biến môi trường hệ thống

# Trong Docker, Airflow chạy tại /opt/airflow
BASE_DIR = "/opt/airflow/Data/data_original"
CONFIG_DIR = "/opt/airflow/config"

USER = os.getenv("COPERNICUS_USER") 
PW = os.getenv("COPERNICUS_PASS")

# --- 🔑 KHỞI TẠO EARTH ENGINE (DOCKER CHUYÊN NGHIỆP) ---
def init_ee():
    try:
        # Đường dẫn file key bên trong Docker
        key_path = "/opt/airflow/config/gee-key.json"
        
        if os.path.exists(key_path):
            # Khởi tạo bằng file JSON
            credentials = ee.ServiceAccountCredentials('', key_path)
            ee.Initialize(credentials)
            logger.info("🔐 GEE initialized via JSON Key file (Professional Setup)")
        else:
            # Dự phòng nếu chạy local ngoài Docker
            ee.Initialize()
            logger.info("🔐 GEE initialized via Default Credentials")
    except Exception as e:
        logger.error(f"❌ GEE Auth Failed: {e}")
        sys.exit(1)

init_ee()

# --- 🗺️ CẤU HÌNH VÙNG CHỌN ---
DANANG_BBOX = [107.90, 15.95, 108.40, 16.25]
ROI = ee.Geometry.Rectangle(DANANG_BBOX)

# Tạo cấu trúc thư mục
for f in ["Static", "Daily/Rain", "Daily/Soil", "Daily/Tide", "Daily/FloodLabel"]: 
    os.makedirs(os.path.join(BASE_DIR, f), exist_ok=True)

# --- 🛠️ HÀM TẢI LOCAL ---
def download_now(image, folder, name, scale):
    path = os.path.join(BASE_DIR, folder, f"{name}.tif")
    if not os.path.exists(path):
        try:
            geemap.download_ee_image(image, filename=path, scale=scale, region=ROI, crs="EPSG:4326")
            logger.info(f"💾 Saved: {name}")
        except Exception as e:
            logger.error(f"❌ Error {name}: {e}")

# ==========================================================
# 🏛️ 1. STATIC LAYERS (Tải 1 lần duy nhất)
# ==========================================================
logger.info("🏛️ Đang chuẩn bị dữ liệu Static...")

dem = ee.Image("USGS/SRTMGL1_003").clip(ROI)
download_now(dem, "Static", "Terrain_DEM_Raw", 30)

# Load metadata chuẩn từ file vừa tải (Dùng để đồng bộ hóa kích thước các layer khác)
dem_path = os.path.join(BASE_DIR, "Static/Terrain_DEM_Raw.tif")
if os.path.exists(dem_path):
    with rasterio.open(dem_path) as src:
        master_meta = src.meta.copy()
        master_transform = src.transform
        master_shape = (src.height, src.width)
    geom_mask = dem.mask()
else:
    logger.error("❌ Không thể tải file Static cơ sở. Pipeline dừng lại.")
    sys.exit(1)

# Các layer tĩnh khác
download_now(ee.Terrain.slope(dem), "Static", "Terrain_Slope_Raw", 30)
download_now(ee.Image("WWF/HydroSHEDS/15ACC").clip(ROI), "Static", "Terrain_Flow_Raw", 30)
download_now(ee.ImageCollection("ESA/WorldCover/v200").first().clip(ROI), "Static", "LandCover_ESA_Raw", 10)

# ==========================================================
# 📅 2. DAILY LAYERS (Airflow Incremental)
# ==========================================================
# Định nghĩa khoảng thời gian cố định theo ý bạn
start_date = "2023-01-01"
end_date = "2025-12-31"

logger.info(f"🚀 BẮT ĐẦU CÀO DỮ LIỆU LỊCH SỬ TỪ {start_date} ĐẾN {end_date}")

# Tạo danh sách ngày
dates = pd.date_range(start_date, end_date).strftime("%Y-%m-%d")

for day in dates:
    curr = ee.Date(day)
    
    # Kiểm tra xem file cuối cùng của ngày đó (SAR hoặc Tide) đã tồn tại chưa để tránh tải lại
    # Ở đây mình check file Rain làm đại diện
    check_path = os.path.join(BASE_DIR, "Daily/Rain", f"Rain_{day}.tif")
    if os.path.exists(check_path):
        # logger.info(f"⏭️  Skip: {day} đã có dữ liệu.")
        continue

    logger.info(f"📅 --- PROCESSING: {day} ---")

    # 🌧️ 1. Rain (CHIRPS)
    rain = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").filterDate(curr, curr.advance(1, 'day')).sum().clip(ROI)
    download_now(rain.updateMask(geom_mask), "Daily/Rain", f"Rain_{day}", 5566)

    # 🌱 2. Soil Moisture (SMAP)
    soil_col = ee.ImageCollection("NASA/SMAP/SPL4SMGP/008") \
                 .filterDate(curr.advance(-1, 'day'), curr.advance(1, 'day')) \
                 .select('sm_surface')
    if soil_col.size().getInfo() > 0:
        download_now(soil_col.mean().clip(ROI).updateMask(geom_mask), "Daily/Soil", f"Soil_{day}", 9000)

    # ⚓ 3. Tide (Copernicus)
    tide_path = os.path.join(BASE_DIR, "Daily/Tide", f"Tide_{day}.tif")
    if not os.path.exists(tide_path):
        try:
            ds = cm.open_dataset(
                dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
                variables=["zos"],
                minimum_longitude=DANANG_BBOX[0], maximum_longitude=DANANG_BBOX[2],
                minimum_latitude=DANANG_BBOX[1], maximum_latitude=DANANG_BBOX[3],
                start_datetime=day, end_datetime=day,
                username=USER, password=PW # Truyền trực tiếp từ .env
            )
            data = ds['zos'].sel(time=day, method="nearest").values
            if len(data.shape) == 3: data = data[0]

            tide_aligned = np.zeros(master_shape, dtype=np.float32)
            raw_h, raw_w = data.shape
            raw_transform = rasterio.transform.from_origin(
                DANANG_BBOX[0], DANANG_BBOX[3], 
                (DANANG_BBOX[2]-DANANG_BBOX[0])/raw_w, (DANANG_BBOX[3]-DANANG_BBOX[1])/raw_h
            )

            reproject(
                source=data.astype(np.float32), destination=tide_aligned,
                src_transform=raw_transform, src_crs="EPSG:4326",
                dst_transform=master_transform, dst_crs="EPSG:4326",
                resampling=Resampling.bilinear
            )

            out_meta = master_meta.copy()
            out_meta.update({"dtype": "float32", "count": 1, "nodata": np.nan})
            with rasterio.open(tide_path, "w", **out_meta) as dst:
                dst.write(tide_aligned, 1)
            ds.close()
            logger.info(f"⚓ Tide Aligned: {day}")
        except Exception as e:
            logger.error(f"⚓ Tide Fail at {day}: {e}")

    # 🌊 4. Flood Label (Sentinel-1 SAR)
    s1_day = ee.ImageCollection('COPERNICUS/S1_GRD') \
               .filterBounds(ROI) \
               .filterDate(curr, curr.advance(1, 'day')) \
               .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))

    if s1_day.size().getInfo() > 0:
        flood_img = s1_day.mosaic().clip(ROI).updateMask(geom_mask)
        download_now(flood_img, "Daily/FloodLabel", f"Flood_SAR_{day}", 10)

logger.info("✨ HOÀN THÀNH: Dữ liệu đã sẵn sàng!")