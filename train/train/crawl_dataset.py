import ee
import geemap
import os
import pandas as pd
import logging
import copernicusmarine as cm
import rasterio
import numpy as np
from dotenv import load_dotenv
from rasterio.warp import reproject, Resampling

# --- 📝 CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FloodLocalMaster")

# --- 🔑 KHỞI TẠO EARTH ENGINE ---
try:
    ee.Initialize()
except:
    ee.Authenticate()
    ee.Initialize()

# --- 📂 CẤU HÌNH ĐƯỜNG DẪN & TÀI KHOẢN ---
load_dotenv() 
USER = os.getenv("COPERNICUS_USER") 
PW = os.getenv("COPERNICUS_PASS")

DANANG_BBOX = [107.90, 15.95, 108.40, 16.25]
ROI = ee.Geometry.Rectangle(DANANG_BBOX)
BASE_DIR = "DaNang_Flood_Local_Raw"

# Tạo cấu trúc thư mục đầy đủ
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
# 🏛️ 1. STATIC LAYERS (Full Features)
# ==========================================================
logger.info("🏛️ Đang chuẩn bị dữ liệu Static đầy đủ...")

# 1.1. DEM (Elevation)
dem = ee.Image("USGS/SRTMGL1_003").clip(ROI)
download_now(dem, "Static", "Terrain_DEM_Raw", 30)

# 1.2. Slope (Độ dốc - Rất quan trọng để biết nước chảy đi đâu)
slope = ee.Terrain.slope(dem)
download_now(slope, "Static", "Terrain_Slope_Raw", 30)

# 1.3. Flow Accumulation (Tích tụ dòng chảy - Xác định lòng sông/suối)
# Sử dụng HydroSHEDS để biết vùng nào là túi chứa nước
flow = ee.Image("WWF/HydroSHEDS/15ACC").clip(ROI)
download_now(flow, "Static", "Terrain_Flow_Raw", 30)

# 1.4. Land Cover (ESA WorldCover - Loại hình che phủ)
# Phân biệt đô thị (ngập nhanh) và rừng (thấm tốt)
landcover = ee.ImageCollection("ESA/WorldCover/v200").first().clip(ROI)
download_now(landcover, "Static", "LandCover_ESA_Raw", 10) # Scale 10m cho chi tiết

# --- Cấu hình Master Meta từ DEM ---
# (Dùng file DEM vừa tải về làm chuẩn để đồng bộ cho Tide và các lớp khác)
dem_path = os.path.join(BASE_DIR, "Static/Terrain_DEM_Raw.tif")
with rasterio.open(dem_path) as src:
    master_meta = src.meta.copy()
    master_transform = src.transform
    master_shape = (src.height, src.width)

geom_mask = dem.mask()

# ==========================================================
# 📅 2. DAILY LAYERS
# ==========================================================
dates = pd.date_range("2020-01-01", "2022-12-31").strftime("%Y-%m-%d")

for day in dates:
    curr = ee.Date(day)
    logger.info(f"📅 --- PROCESSING: {day} ---")

    # 🌧️ 1. Rain (CHIRPS)
    rain = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").filterDate(curr, curr.advance(1, 'day')).sum().clip(ROI)
    download_now(rain.updateMask(geom_mask), "Daily/Rain", f"Rain_{day}", 5566)

    # 🌱 2. Soil Moisture (SMAP) - ĐÃ THÊM LẠI
    soil_col = ee.ImageCollection("NASA/SMAP/SPL4SMGP/008") \
                 .filterDate(curr.advance(-1, 'day'), curr.advance(1, 'day')) \
                 .select('sm_surface')
    if soil_col.size().getInfo() > 0:
        soil_img = soil_col.mean().clip(ROI).updateMask(geom_mask)
        download_now(soil_img, "Daily/Soil", f"Soil_{day}", 9000)
    else:
        logger.warning(f"⚠️ No Soil data for {day}")

    # ⚓ 3. Tide (Copernicus - Xử lý Offline)
    tide_path = os.path.join(BASE_DIR, "Daily/Tide", f"Tide_{day}.tif")
    if not os.path.exists(tide_path):
        try:
            ds = cm.open_dataset(
                dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
                variables=["zos"],
                minimum_longitude=DANANG_BBOX[0], maximum_longitude=DANANG_BBOX[2],
                minimum_latitude=DANANG_BBOX[1], maximum_latitude=DANANG_BBOX[3],
                start_datetime=day, end_datetime=day
            )
            data = ds['zos'].sel(time=day, method="nearest").values
            if len(data.shape) == 3: data = data[0]

            raw_h, raw_w = data.shape
            raw_transform = rasterio.transform.from_origin(
                DANANG_BBOX[0], DANANG_BBOX[3], 
                (DANANG_BBOX[2]-DANANG_BBOX[0])/raw_w, (DANANG_BBOX[3]-DANANG_BBOX[1])/raw_h
            )

            tide_aligned = np.zeros(master_shape, dtype=np.float32)
            reproject(
                source=data.astype(np.float32),
                destination=tide_aligned,
                src_transform=raw_transform,
                src_crs="EPSG:4326",
                dst_transform=master_transform,
                dst_crs="EPSG:4326",
                resampling=Resampling.bilinear
            )

            out_meta = master_meta.copy()
            out_meta.update({"dtype": "float32", "count": 1, "nodata": np.nan})
            with rasterio.open(tide_path, "w", **out_meta) as dst:
                dst.write(tide_aligned, 1)
            
            ds.close()
            logger.info(f"⚓ Tide Grid Aligned: {day}")
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

logger.info("✨ HOÀN THÀNH: Rain, Soil, Tide, SAR đã sẵn sàng!")