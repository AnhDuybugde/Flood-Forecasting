import ee
import geemap
import os
import rasterio
import numpy as np
import pandas as pd
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import datetime
from rasterio.warp import reproject, Resampling
from rasterio.merge import merge
from rasterio.crs import CRS as rio_CRS
import copernicusmarine as cm
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))
from config import USERNAME, PASSWORD

# --- ⚙️ CONFIG & OPTIONS ---
load_dotenv(os.path.join(os.path.dirname(__file__), 'backend', '.env'))
CLEAN_LOCAL = True  # Xoá file .tif ở local DaNang/ sau khi upload thành công lên R2
MAX_WORKERS = 20    # Tăng tối đa số luồng chạy song song (Phụ thuộc cấu hình RAM/CPU và giới hạn API)

# R2 Cloudflare Setup
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_BUCKET = os.environ.get('R2_BUCKET_NAME', 'satellite-data-10x10')

s3_client = None
if R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY:
    s3_client = boto3.client(
        's3',
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto"
    )

# --- 📝 LOGGING ---
class CustomFormatter(logging.Formatter):
    def format(self, record):
        icons = {logging.INFO: "ℹ️", logging.WARNING: "⚠️", logging.ERROR: "❌"}
        icon = icons.get(record.levelno, "•")
        return logging.Formatter(f"{icon} %(asctime)s - %(message)s", datefmt='%H:%M:%S').format(record)

logger = logging.getLogger("FloodPipeline_Fast")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler(); ch.setFormatter(CustomFormatter()); logger.addHandler(ch)

# --- 🚀 INIT ---
try:
    ee_project = os.environ.get('EE_PROJECT_ID')
    if not ee_project:
        raise ValueError("Lỗi: Không tìm thấy biến môi trường EE_PROJECT_ID trong .env")
    ee.Initialize(project=ee_project)
except ee.ee_exception.EEException as e:
    logger.error(f"⚠️ EE Init Error: {e}")
    raise e

BASE_DIR = "DaNang"
CRS_TARGET = "EPSG:4326"
SCALE = 10
DANANG_BBOX = [107.90, 15.95, 108.40, 16.25]
ROI = ee.Geometry.Rectangle(DANANG_BBOX)

for sub in ["Static", "Daily/Rain", "Daily/Soil", "Daily/Tide", "Daily/Flood"]:
    os.makedirs(os.path.join(BASE_DIR, sub), exist_ok=True)

# --- 🛠️ HELPERS ---
def write_int16(path, data, meta, scale=1000, nodata=-9999):
    data_cleaned = np.nan_to_num(data, nan=nodata, posinf=32767, neginf=nodata)
    data_scaled = (data_cleaned * scale).astype("int16")
    meta_copy = meta.copy() # TRÁNH THREAD CLASH BIẾN GLOBAL
    meta_copy.update({"dtype": "int16", "compress": "lzw", "nodata": nodata, "predictor": 2})
    with rasterio.open(path, "w", **meta_copy) as dst:
        dst.write(data_scaled, 1)

def to_master(src_path, out_path, master_meta):
    if not os.path.exists(src_path) or master_meta is None: return
    with rasterio.open(src_path) as s:
        dest_arr = np.zeros((master_meta['height'], master_meta['width']), dtype=np.float32)
        reproject(
            source=s.read(1), destination=dest_arr,
            src_transform=s.transform, src_crs=s.crs if s.crs else CRS_TARGET,
            dst_transform=master_meta['transform'], dst_crs=master_meta['crs'],
            resampling=Resampling.nearest
        )
        write_int16(out_path, dest_arr, master_meta)

def get_tiled_static(ee_image, out_path, scale=SCALE, is_dem=False):
    x_coords = np.linspace(DANANG_BBOX[0], DANANG_BBOX[2], 3)
    y_coords = np.linspace(DANANG_BBOX[1], DANANG_BBOX[3], 3)
    tile_files = []
    
    try:
        for i in range(2):
            for j in range(2):
                tile_bbox = [x_coords[i], y_coords[j], x_coords[i+1], y_coords[j+1]]
                tile_geom = ee.Geometry.Rectangle(tile_bbox)
                tile_name = f"temp_static_{i}_{j}.tif"
                geemap.ee_export_image(ee_image.clip(tile_geom), filename=tile_name, scale=scale, region=tile_geom, crs=CRS_TARGET)
                if os.path.exists(tile_name):
                    tile_files.append(tile_name)
        
        if len(tile_files) < 4: raise Exception("Not all tiles downloaded successfully.")

        src_files = [rasterio.open(f) for f in tile_files]
        mosaic, out_trans = merge(src_files)
        meta = src_files[0].meta.copy()
        meta.update({"height": mosaic.shape[1], "width": mosaic.shape[2], "transform": out_trans, "crs": rio_CRS.from_epsg(4326)})
        
        write_int16(out_path, mosaic[0], meta, scale=1 if is_dem else 1)
        for f in src_files: f.close()
        return meta
    finally:
        for f in tile_files:
            if os.path.exists(f): os.remove(f)

# --- ☁️ CLOUD FUNCTIONS ---
def check_r2_exists(r2_key):
    if not s3_client: return False
    try:
        s3_client.head_object(Bucket=R2_BUCKET, Key=r2_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        return False

def upload_to_r2(local_path, r2_key):
    if not s3_client: return False
    if not os.path.exists(local_path): return False
    try:
        s3_client.upload_file(local_path, R2_BUCKET, r2_key)
        logger.info(f"  ☁️ Uploaded to R2: {r2_key}")
        if CLEAN_LOCAL:
            os.remove(local_path)
            logger.info(f"  🧹 Cleaned local: {local_path}")
        return True
    except Exception as e:
        logger.error(f"  ❌ Upload Failed ({r2_key}): {e}")
        return False

# --- 🚦 WORKER FUNCTION (Xử lý 1 ngày độc lập) ---
def process_single_day(day, master_meta):
    logger.info(f"⏳ Bắt đầu tải ngày: {day}")
    
    files = {
        "Rain":  {"local": f"{BASE_DIR}/Daily/Rain/Rain_{day}.tif", "r2": f"FloodData/{BASE_DIR}/Daily/Rain/Rain_{day}.tif"},
        "Soil":  {"local": f"{BASE_DIR}/Daily/Soil/Soil_{day}.tif", "r2": f"FloodData/{BASE_DIR}/Daily/SoilMoisture/SoilMoisture_{day}.tif"},
        "Tide":  {"local": f"{BASE_DIR}/Daily/Tide/Tide_{day}.tif", "r2": f"FloodData/{BASE_DIR}/Daily/Tide/Tide_{day}.tif"},
        "Flood": {"local": f"{BASE_DIR}/Daily/Flood/Flood_{day}.tif", "r2": f"FloodData/{BASE_DIR}/LabelDaily/Flood_{day}.tif"}
    }
    
    # Định nghĩa tên file temp chứa mã ngày để chống đụng độ giữa các luồng (Thread Clash)
    t_rain_file = f"temp_rain_{day}.tif"
    t_soil_file = f"temp_soil_{day}.tif"
    t_tide_file = f"temp_tide_{day}.tif"
    t_flood_file = f"temp_flood_{day}.tif"

    # --- 1. RAIN 🌧️ ---
    if not check_r2_exists(files["Rain"]["r2"]):
        if not os.path.exists(files["Rain"]["local"]):
            try:
                rain = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").filterDate(day).sum().clip(ROI)
                geemap.ee_export_image(rain, filename=t_rain_file, scale=5000, region=ROI, crs=CRS_TARGET, verbose=False)
                to_master(t_rain_file, files["Rain"]["local"], master_meta)
            except Exception as e: logger.error(f"[{day}] 🌧️ Rain Fail: {e}")
        if os.path.exists(files["Rain"]["local"]):
            upload_to_r2(files["Rain"]["local"], files["Rain"]["r2"])
    else: 
        if CLEAN_LOCAL and os.path.exists(files["Rain"]["local"]): os.remove(files["Rain"]["local"])

    # --- 2. SOIL MOISTURE 🌱 ---
    if not check_r2_exists(files["Soil"]["r2"]):
        if not os.path.exists(files["Soil"]["local"]):
            try:
                start_search = ee.Date(day).advance(-1, 'day')
                end_search = ee.Date(day).advance(1, 'day')
                soil_col = ee.ImageCollection("NASA/SMAP/SPL4SMGP/008").filterDate(start_search, end_search).select("sm_surface")
                
                if soil_col.size().getInfo() > 0:
                    soil_img = soil_col.mean().clip(ROI)
                    geemap.ee_export_image(soil_img, filename=t_soil_file, scale=9000, region=ROI, crs=CRS_TARGET, verbose=False)
                    to_master(t_soil_file, files["Soil"]["local"], master_meta)
            except Exception as e: logger.error(f"[{day}] 🌱 Soil Fail: {e}")
        if os.path.exists(files["Soil"]["local"]):
            upload_to_r2(files["Soil"]["local"], files["Soil"]["r2"])
    else: 
        if CLEAN_LOCAL and os.path.exists(files["Soil"]["local"]): os.remove(files["Soil"]["local"])

    # --- 3. TIDE ⚓ ---
    if not check_r2_exists(files["Tide"]["r2"]):
        if not os.path.exists(files["Tide"]["local"]):
            try:
                ds = cm.open_dataset(dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m", username=USERNAME, password=PASSWORD, variables=["zos"],
                                     minimum_longitude=DANANG_BBOX[0], maximum_longitude=DANANG_BBOX[2],
                                     minimum_latitude=DANANG_BBOX[1], maximum_latitude=DANANG_BBOX[3],
                                     start_datetime=day, end_datetime=day)
                data = ds['zos'].sel(time=day, method="nearest").values
                data = data[0] if len(data.shape) == 3 else data
                trans = rasterio.transform.from_origin(DANANG_BBOX[0], DANANG_BBOX[3], (DANANG_BBOX[2]-DANANG_BBOX[0])/data.shape[1], (DANANG_BBOX[3]-DANANG_BBOX[1])/data.shape[0])
                with rasterio.open(t_tide_file, "w", driver="GTiff", height=data.shape[0], width=data.shape[1], count=1, dtype="float32", crs=CRS_TARGET, transform=trans) as dst:
                    dst.write(data.astype("float32"), 1)
                to_master(t_tide_file, files["Tide"]["local"], master_meta)
                ds.close()
            except Exception as e: logger.error(f"[{day}] ⚓ Tide Fail: {e}")
        if os.path.exists(files["Tide"]["local"]):
            upload_to_r2(files["Tide"]["local"], files["Tide"]["r2"])
    else: 
        if CLEAN_LOCAL and os.path.exists(files["Tide"]["local"]): os.remove(files["Tide"]["local"])

    # --- 4. FLOOD 🌊 ---
    if not check_r2_exists(files["Flood"]["r2"]):
        if not os.path.exists(files["Flood"]["local"]):
            try:
                flood_img = ee.Image("JRC/GSW1_4/GlobalSurfaceWater").select('occurrence').clip(ROI).unmask(0)
                geemap.ee_export_image(flood_img, filename=t_flood_file, scale=20, region=ROI, crs=CRS_TARGET, verbose=False)
                to_master(t_flood_file, files["Flood"]["local"], master_meta)
            except Exception as e: logger.error(f"[{day}] 🌊 Flood Fail: {e}")
        if os.path.exists(files["Flood"]["local"]):
            upload_to_r2(files["Flood"]["local"], files["Flood"]["r2"])
    else: 
        if CLEAN_LOCAL and os.path.exists(files["Flood"]["local"]): os.remove(files["Flood"]["local"])

    # Dọn dẹp cache temp file
    for p in [t_rain_file, t_soil_file, t_flood_file, t_tide_file]:
        if os.path.exists(p): os.remove(p)
        
    logger.info(f"✅ Đã xử lý xong toàn bộ dữ liệu ngày: {day}")

# ==============================================================
#                      HÀM MAIN KHỞI CHẠY                        
# ==============================================================
if __name__ == '__main__':
    master_meta = None
    dem_path = os.path.join(BASE_DIR, "Static/DEM.tif")
    r2_dem_key = f"FloodData/{BASE_DIR}/Static/DEM.tif"

    logger.info("🏛️ Processing Static Layers (Tuần tự)...")
    dem_exists_r2 = check_r2_exists(r2_dem_key)

    if not os.path.exists(dem_path) and not dem_exists_r2:
        master_meta = get_tiled_static(ee.Image("USGS/SRTMGL1_003").float(), dem_path, is_dem=True)
        upload_to_r2(dem_path, r2_dem_key)
    elif os.path.exists(dem_path):
        with rasterio.open(dem_path) as src: master_meta = src.meta.copy()
        if not dem_exists_r2: upload_to_r2(dem_path, r2_dem_key)
    else:
        logger.info("  🏛️ DEM: SKIPPED (Exists on R2)")
        if master_meta is None:
            logger.warning("  ⚠️ Master DEM is on R2 but not local. Temporarily downloading for reference...")
            try:
                s3_client.download_file(R2_BUCKET, r2_dem_key, dem_path)
                with rasterio.open(dem_path) as src: master_meta = src.meta.copy()
                if CLEAN_LOCAL: os.remove(dem_path)
            except Exception as e:
                logger.error(f"  ❌ Failed to download master DEM from R2: {e}")

    static_list = [
        ("LandCover", ee.ImageCollection("ESA/WorldCover/v100").first().select('Map').float()),
        ("Slope", ee.Terrain.slope(ee.Image("USGS/SRTMGL1_003")).float()),
        ("Flow", ee.Image("WWF/HydroSHEDS/15ACC").float())
    ]

    for name, img in static_list:
        path = os.path.join(BASE_DIR, f"Static/{name}.tif")
        r2_key = f"FloodData/{BASE_DIR}/Static/{name}.tif"
        if not check_r2_exists(r2_key):
            if not os.path.exists(path):
                logger.info(f"📡 Exporting Static: {name}")
                try:
                    get_tiled_static(img.clip(ROI), path, scale=SCALE)
                except Exception as e:
                    logger.error(f"❌ {name} Fail: {e}")
            if os.path.exists(path):
                upload_to_r2(path, r2_key)
                logger.info(f"✅ {name}: OK")
        else:
            logger.info(f"✅ {name}: SKIPPED (Exists on R2)")
            if CLEAN_LOCAL and os.path.exists(path):
                os.remove(path)

    # --- 📅 BẮT ĐẦU VÒNG LẶP DAILY ĐA LUỒNG ---
    end_date_str = datetime.now().strftime("%Y-%m-%d")
    dates = pd.date_range(start="2020-01-01", end=end_date_str).strftime("%Y-%m-%d")

    logger.info(f"\n🚀 Kích hoạt ThreadPoolExecutor với {MAX_WORKERS} Threads 🚀")
    
    # Kích hoạt đa luồng (Multi-threading)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_single_day, day, master_meta) for day in dates]
        
        # Theo dõi tiến độ hoàn thành
        for future in as_completed(futures):
            try:
                future.result() # Nếu hàm xảy ra exception không bắt được, nó sẽ văng ra ở đây
            except Exception as exc:
                logger.error(f"Văng luồng do Exception không mong muốn: {exc}")

    logger.info("✨ Pipeline Finished! ✨")
