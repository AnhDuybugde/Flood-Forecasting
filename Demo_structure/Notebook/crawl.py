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
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 📝 CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FloodLocalMaster")

# --- 📂 CẤU HÌNH ĐƯỜNG DẪN & TÀI KHOẢN ---
load_dotenv() 
# Sửa lại tên biến để tránh bị prompt hỏi password liên tục
USER = os.getenv("jloy541411@gmail.com") 
PW = os.getenv("ADuy05042006@")

# --- 🔑 KHỞI TẠO EARTH ENGINE ---
try:
    ee.Initialize(project='landsurface-485908')
    logger.info("🔐 GEE initialized successfully.")
except Exception as e:
    logger.warning(f"⚠️ GEE chưa được xác thực. Lỗi: {e}")
    logger.info("Mở trình duyệt để xác thực GEE...")
    try:
        ee.Authenticate()
        ee.Initialize(project='landsurface-485908')
        logger.info("🔐 GEE authenticated and initialized.")
    except Exception as auth_error:
        logger.error(f"❌ Không thể xác thực GEE: {auth_error}")
        logger.info("💡 MẸO: Hãy mở terminal (PowerShell) và gõ lệnh: earthengine authenticate")
        sys.exit(1)
        
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
    else:
        logger.info(f"⏭️  Đã tồn tại (Bỏ qua): {name}")

# ==========================================================
# 🏛️ 1. STATIC LAYERS (Tuần tự - Bắt buộc)
# ==========================================================
logger.info("🏛️ Đang chuẩn bị dữ liệu Static đầy đủ...")

dem = ee.Image("USGS/SRTMGL1_003").clip(ROI)
download_now(dem, "Static", "Terrain_DEM_Raw", 30)

slope = ee.Terrain.slope(dem)
download_now(slope, "Static", "Terrain_Slope_Raw", 30)

flow = ee.Image("WWF/HydroSHEDS/15ACC").clip(ROI)
download_now(flow, "Static", "Terrain_Flow_Raw", 30)

landcover = ee.ImageCollection("ESA/WorldCover/v200").first().clip(ROI)
download_now(landcover, "Static", "LandCover_ESA_Raw", 10)

# --- Cấu hình Master Meta từ DEM ---
dem_path = os.path.join(BASE_DIR, "Static/Terrain_DEM_Raw.tif")
if os.path.exists(dem_path):
    with rasterio.open(dem_path) as src:
        master_meta = src.meta.copy()
        master_transform = src.transform
        master_shape = (src.height, src.width)
else:
    logger.error("❌ File DEM chưa được tải xuống. Không thể đồng bộ hệ tọa độ. Dừng chương trình.")
    sys.exit(1)

geom_mask = dem.mask()

# ==========================================================
# 📅 2. DAILY LAYERS (ĐA LUỒNG - PARALLEL PROCESSING)
# ==========================================================

# Tách logic xử lý của 1 ngày ra thành 1 hàm độc lập
def process_single_day(day):
    curr = ee.Date(day)
    logger.info(f"🚀 --- STARTING: {day} ---")

    # 🌧️ 1. Rain (CHIRPS)
    rain = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").filterDate(curr, curr.advance(1, 'day')).sum().clip(ROI)
    download_now(rain.updateMask(geom_mask), "Daily/Rain", f"Rain_{day}", 5566)

    # 🌱 2. Soil Moisture (SMAP)
    soil_col = ee.ImageCollection("NASA/SMAP/SPL4SMGP/008") \
                 .filterDate(curr.advance(-1, 'day'), curr.advance(1, 'day')) \
                 .select('sm_surface')
    if soil_col.size().getInfo() > 0:
        soil_img = soil_col.mean().clip(ROI).updateMask(geom_mask)
        download_now(soil_img, "Daily/Soil", f"Soil_{day}", 9000)
    else:
        logger.warning(f"⚠️ No Soil data for {day}")

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
                username=USER, 
                password=PW 
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

    return day

# Chạy đa luồng
dates = pd.date_range("2020-01-01", "2025-12-31").strftime("%Y-%m-%d")
MAX_WORKERS = 100

logger.info(f"⚡ BẮT ĐẦU CÀO DỮ LIỆU ĐA LUỒNG ({MAX_WORKERS} luồng)...")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Giao việc (submit) cho các luồng
    futures = {executor.submit(process_single_day, day): day for day in dates}
    
    # Lắng nghe kết quả khi từng luồng hoàn thành
    for future in as_completed(futures):
        day = futures[future]
        try:
            future.result()
            logger.info(f"✅ Đã xử lý xong toàn bộ dữ liệu ngày: {day}")
        except Exception as exc:
            logger.error(f"❌ Ngày {day} gặp lỗi nghiêm trọng: {exc}")

logger.info("✨ HOÀN THÀNH: Rain, Soil, Tide, SAR đã sẵn sàng!")