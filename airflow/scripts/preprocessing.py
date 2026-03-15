import os
import sys
import glob
import rasterio
import numpy as np
import pandas as pd
import logging
from tqdm import tqdm
from datetime import datetime, timedelta
from scipy.ndimage import uniform_filter
from rasterio.enums import Resampling
from rasterio.warp import reproject

# --- 📝 CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Preprocess_Pipeline")

# =================================================================
# 1. CẤU HÌNH HỆ THỐNG & ĐƯỜNG DẪN ĐỘNG (AIRFLOW READY)
# =================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Cấu trúc: DAP391M/airflow/scripts/preprocessing.py
# Suy ra thư mục airflow nằm ở cấp cha của scripts
AIRFLOW_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

BASE_DIR = os.path.join(AIRFLOW_DIR, "data")
OUT_DIR = os.path.join(AIRFLOW_DIR, "data_processed")
TRAIN_DIR = os.path.join(AIRFLOW_DIR, "Data_Training_Soft_NPZ")
MASTER_DEM_RAW = os.path.join(BASE_DIR, "Static/Terrain_DEM_Raw.tif")

# Tạo cấu trúc thư mục đích
sub_dirs = [
    "Static", "Daily/Rain", "Daily/Soil", "Daily/Tide", 
    "Daily/FloodLabel", "Daily/FloodMask", "Daily/SAR_Denoised", "Daily/Final_Labels"
]
for d in sub_dirs:
    os.makedirs(os.path.join(OUT_DIR, d), exist_ok=True)
os.makedirs(TRAIN_DIR, exist_ok=True)

# Lấy thông số chuẩn (Master) từ DEM gốc
if not os.path.exists(MASTER_DEM_RAW):
    logger.error(f"❌ Không tìm thấy file Master DEM tại {MASTER_DEM_RAW}. Vui lòng chạy crawl.py trước!")
    sys.exit(1)

with rasterio.open(MASTER_DEM_RAW) as src:
    master_meta = src.meta.copy()
    master_shape = (src.height, src.width)
    master_transform = src.transform
    master_crs = src.crs

# =================================================================
# 2. CÁC HÀM XỬ LÝ LÕI (Giữ nguyên logic của bạn)
# =================================================================

def process_generic_file(in_path, out_path, is_label=False, is_climate=False):
    if not os.path.exists(in_path): 
        return False
    
    with rasterio.open(in_path) as src:
        data_raw = src.read(1).astype('float32')
        data_raw[data_raw < -1000] = 0
        data_raw = np.nan_to_num(data_raw, nan=0.0)

        data_aligned = np.zeros(master_shape, dtype=np.float32)
        resample_alg = Resampling.nearest if is_label else Resampling.bilinear
        
        reproject(
            source=data_raw,
            destination=data_aligned,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=master_transform,
            dst_crs=master_crs,
            resampling=resample_alg,
            dst_nodata=0
        )

        if is_climate:
            data_aligned[data_aligned < 0] = 0

        new_meta = master_meta.copy()
        new_meta.update({
            "dtype": 'float32', "nodata": 0, "count": 1, 
            "width": master_shape[1], "height": master_shape[0]
        })
        
        with rasterio.open(out_path, "w", **new_meta) as dst:
            dst.write(data_aligned, 1)
    return True

def lee_filter(img, size=5):
    img_mean = uniform_filter(img, (size, size))
    img_sqr_mean = uniform_filter(img**2, (size, size))
    img_variance = img_sqr_mean - img_mean**2
    overall_variance = np.var(img)
    img_weights = img_variance / (img_variance + overall_variance + 1e-8)
    return img_mean + img_weights * (img - img_mean)

def smart_normalize(data, method='minmax', global_min=None, global_max=None):
    valid_mask = (data != 0) & (np.isfinite(data))
    if not np.any(valid_mask): return np.zeros_like(data)

    v_min = global_min if global_min is not None else np.min(data[valid_mask])
    v_max = global_max if global_max is not None else np.max(data[valid_mask])

    if method == 'robust' and global_max is None:
        v_max = np.percentile(data[valid_mask], 98) 
    
    denom = v_max - v_min
    if denom == 0: return np.zeros_like(data)
        
    norm_data = (data - v_min) / (denom + 1e-8)
    return np.clip(norm_data, 0, 1).astype('float32')

# =================================================================
# 3. PIPELINE THỰC THI THEO NGÀY (AIRFLOW DAG)
# =================================================================

def process_static_data():
    """Chỉ chạy 1 lần nếu dữ liệu tĩnh chưa được xử lý"""
    check_file = os.path.join(OUT_DIR, "Static/Terrain_DEM_Proc.tif")
    if os.path.exists(check_file):
        logger.info("🏛️ Dữ liệu Static đã tồn tại. Bỏ qua xử lý Static.")
        return

    logger.info("🏛️ Bắt đầu xử lý dữ liệu Static...")
    static_map = {
        "Static/Terrain_DEM_Raw.tif": "Static/Terrain_DEM_Proc.tif",
        "Static/Terrain_Slope_Raw.tif": "Static/Terrain_Slope_Proc.tif",
        "Static/Terrain_Flow_Raw.tif": "Static/Terrain_Flow_Proc.tif",
        "Static/LandCover_ESA_Raw.tif": "Static/LandCover_ESA_Proc.tif"
    }
    for raw_f, proc_f in static_map.items():
        process_generic_file(os.path.join(BASE_DIR, raw_f), os.path.join(OUT_DIR, proc_f), is_label=("LandCover" in raw_f))

def process_daily_data(date_str, lag_days=2):
    logger.info(f"🔄 --- ĐANG XỬ LÝ DỮ LIỆU NGÀY: {date_str} ---")
    
    # --- BƯỚC 1: ALIGNING CHO NGÀY HIỆN TẠI ---
    process_generic_file(os.path.join(BASE_DIR, f"Daily/Rain/Rain_{date_str}.tif"), 
                         os.path.join(OUT_DIR, f"Daily/Rain/Rain_{date_str}.tif"), is_climate=True)
    process_generic_file(os.path.join(BASE_DIR, f"Daily/Soil/Soil_{date_str}.tif"), 
                         os.path.join(OUT_DIR, f"Daily/Soil/Soil_{date_str}.tif"), is_climate=True)
    process_generic_file(os.path.join(BASE_DIR, f"Daily/Tide/Tide_{date_str}.tif"), 
                         os.path.join(OUT_DIR, f"Daily/Tide/Tide_{date_str}.tif"))
    
    # SAR có thể không có mỗi ngày (do vệ tinh 6-12 ngày mới bay qua 1 lần)
    has_sar = process_generic_file(os.path.join(BASE_DIR, f"Daily/FloodLabel/Flood_SAR_{date_str}.tif"), 
                                   os.path.join(OUT_DIR, f"Daily/FloodLabel/Flood_SAR_{date_str}.tif"), is_label=True)

    # --- BƯỚC 2: KHỬ NHIỄU SAR VÀ TẠO MASK (Chỉ chạy nếu có SAR) ---
    if has_sar:
        s_path = os.path.join(OUT_DIR, f"Daily/FloodLabel/Flood_SAR_{date_str}.tif")
        with rasterio.open(s_path) as src:
            data = src.read(1).astype('float32')
            valid_mask = np.where((data != 0) & (np.isfinite(data)), 1, 0).astype('float32')
            
            if np.sum(valid_mask) / valid_mask.size > 0.05:
                # Lưu Mask
                m_path = os.path.join(OUT_DIR, f"Daily/FloodMask/Flood_SAR_{date_str}_mask.tif")
                meta = src.meta.copy()
                meta.update(dtype='float32', nodata=0)
                with rasterio.open(m_path, "w", **meta) as dst: dst.write(valid_mask, 1)
                
                # Khử nhiễu và lưu
                denoised = lee_filter(data)
                d_path = os.path.join(OUT_DIR, f"Daily/SAR_Denoised/Flood_SAR_{date_str}.tif")
                with rasterio.open(d_path, "w", **meta) as dst: dst.write(denoised, 1)
            else:
                os.remove(s_path) # Xóa nếu ảnh quá ít pixel hợp lệ
                has_sar = False
                logger.warning(f"⚠️ Dữ liệu SAR ngày {date_str} có quá ít pixel hợp lệ. Đã loại bỏ.")

    # --- BƯỚC 3: CHUẨN HÓA (Chỉ xử lý file của ngày hôm nay) ---
    logger.info(f"📊 Đang chuẩn hóa features ngày {date_str}...")
    
    # Chuẩn hóa Rain & Soil
    for feature, method in [("Rain", "robust"), ("Soil", "minmax")]:
        f_path = os.path.join(OUT_DIR, f"Daily/{feature}/{feature}_{date_str}.tif")
        if os.path.exists(f_path):
            with rasterio.open(f_path) as src:
                data, prof = src.read(1).astype('float32'), src.profile
                norm_d = smart_normalize(data, method=method)
                with rasterio.open(f_path, "w", **prof) as dst: dst.write(norm_d, 1)

    # LƯU Ý: Với Tide, do chạy daily nên tạm thời chuẩn hóa MinMax trong ngày. 
    # Nếu muốn chuẩn xác model, bạn nên fix cứng global_min = -2, global_max = 2 (tùy khu vực Đà Nẵng)
    t_path = os.path.join(OUT_DIR, f"Daily/Tide/Tide_{date_str}.tif")
    if os.path.exists(t_path):
        with rasterio.open(t_path) as src:
            data, prof = src.read(1).astype('float32'), src.profile
            norm_d = smart_normalize(data, method='minmax') # Hoặc truyền global_min/max vào đây
            with rasterio.open(t_path, "w", **prof) as dst: dst.write(norm_d, 1)

    if has_sar:
        sar_path = os.path.join(OUT_DIR, f"Daily/SAR_Denoised/Flood_SAR_{date_str}.tif")
        if os.path.exists(sar_path):
            with rasterio.open(sar_path) as src:
                data, prof = src.read(1).astype('float32'), src.profile
                norm_d = smart_normalize(data, method='minmax')
                with rasterio.open(sar_path, "w", **prof) as dst: dst.write(norm_d, 1)

    # --- BƯỚC 4: ĐÓNG GÓI NPZ (Chỉ chạy khi có SAR) ---
    if has_sar:
        logger.info(f"📦 Đang đóng gói dữ liệu NPZ cho ngày {date_str}...")
        stack_training_data_for_day(date_str, lag_days)
    else:
        logger.info(f"⏩ Ngày {date_str} không có ảnh SAR. Bỏ qua bước đóng gói NPZ.")

def stack_training_data_for_day(date_str, lag_days=2):
    current_date = datetime.strptime(date_str, "%Y-%m-%d")
    
    # Kiểm tra xem có đủ dữ liệu Rain của các ngày trước không (lag_days)
    rain_series = []
    for i in range(lag_days + 1):
        t_date = current_date - timedelta(days=i)
        r_f = os.path.join(OUT_DIR, f"Daily/Rain/Rain_{t_date.strftime('%Y-%m-%d')}.tif")
        if not os.path.exists(r_f):
            logger.warning(f"❌ Thiếu dữ liệu Mưa ngày {t_date.strftime('%Y-%m-%d')}. Không thể đóng gói NPZ.")
            return
        with rasterio.open(r_f) as src: rain_series.append(src.read(1))
    
    try:
        # Load Y (Soft Label)
        s_path = os.path.join(OUT_DIR, f"Daily/SAR_Denoised/Flood_SAR_{date_str}.tif")
        m_path = os.path.join(OUT_DIR, f"Daily/FloodMask/Flood_SAR_{date_str}_mask.tif")
        
        with rasterio.open(s_path) as src_sar, rasterio.open(m_path) as src_mask:
            sar_val = src_sar.read(1).astype('float32')
            v_mask = src_mask.read(1)
            valid_pixels = sar_val[v_mask == 1]
            if len(valid_pixels) == 0: return
            
            water_ref, land_ref = np.percentile(valid_pixels, 2), np.percentile(valid_pixels, 98)
            Y = np.clip((sar_val - land_ref) / (water_ref - land_ref + 1e-8), 0, 1) * v_mask

        # Load X features
        with rasterio.open(os.path.join(OUT_DIR, f"Daily/Soil/Soil_{date_str}.tif")) as src: soil = src.read(1)
        with rasterio.open(os.path.join(OUT_DIR, f"Daily/Tide/Tide_{date_str}.tif")) as src: tide = src.read(1)
        with rasterio.open(os.path.join(OUT_DIR, "Static/Terrain_DEM_Proc.tif")) as src: dem = src.read(1)
        with rasterio.open(os.path.join(OUT_DIR, "Static/Terrain_Slope_Proc.tif")) as src: slope = src.read(1)
        with rasterio.open(os.path.join(OUT_DIR, "Static/Terrain_Flow_Proc.tif")) as src: flow = src.read(1)

        X = np.stack([*rain_series, soil, tide, dem, slope, flow], axis=0).astype('float32')
        out_npz = os.path.join(TRAIN_DIR, f"Sample_{date_str}.npz")
        np.savez_compressed(out_npz, x=X, y=Y)
        logger.info(f"✅ Đã tạo thành công: {out_npz}")
    except Exception as e:
        logger.error(f"❌ Lỗi khi stack NPZ ngày {date_str}: {e}")

if __name__ == "__main__":
    # Nhận tham số từ Airflow (tương tự crawl.py)
    if len(sys.argv) == 2:
        start_date = sys.argv[1]
        end_date = sys.argv[1]
    elif len(sys.argv) == 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        yesterday = pd.Timestamp.now() - pd.Timedelta(days=1)
        start_date = yesterday.strftime("%Y-%m-%d")
        end_date = start_date

    dates = pd.date_range(start_date, end_date).strftime("%Y-%m-%d")
    
    # 1. Chạy xử lý file tĩnh (nếu chưa có)
    process_static_data()
    
    # 2. Xử lý dữ liệu thay đổi theo từng ngày
    for day in dates:
        process_daily_data(day)