import os
import sys
import rasterio
import numpy as np
import logging
from datetime import datetime, timedelta
from scipy.ndimage import uniform_filter
from rasterio.enums import Resampling
from rasterio.warp import reproject

# --- 📝 CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Preprocess_Pipeline")

DATA_ROOT = "/opt/airflow/Data"
BASE_DIR = os.path.join(DATA_ROOT, "data_original")
OUT_DIR = os.path.join(DATA_ROOT, "data_processed")
TRAIN_DIR = os.path.join(DATA_ROOT, "data_training")

# =================================================================
# 1. HÀM TIỆN ÍCH
# =================================================================

def lee_filter(img, size=5):
    img_mean = uniform_filter(img, (size, size))
    img_sqr_mean = uniform_filter(img**2, (size, size))
    img_variance = img_sqr_mean - img_mean**2
    overall_variance = np.var(img)
    img_weights = img_variance / (img_variance + overall_variance + 1e-8)
    return img_mean + img_weights * (img - img_mean)

def read_raster(path):
    """Đọc raster và trả về mảng 2D"""
    if not os.path.exists(path):
        return None
    with rasterio.open(path) as src:
        data = src.read(1).astype('float32')
        return np.nan_to_num(data, nan=0.0)

def process_generic_file(in_path, out_path, master_meta, master_shape, is_label=False):
    """Align và lưu ảnh về khung chuẩn của DEM"""
    if not os.path.exists(in_path): return False
    with rasterio.open(in_path) as src:
        data_raw = src.read(1).astype('float32')
        data_raw = np.nan_to_num(data_raw, nan=0.0)
        
        data_aligned = np.zeros(master_shape, dtype=np.float32)
        resample_alg = Resampling.nearest if is_label else Resampling.bilinear
        
        reproject(
            source=data_raw, destination=data_aligned,
            src_transform=src.transform, src_crs=src.crs,
            dst_transform=master_meta['transform'], dst_crs=master_meta['crs'],
            resampling=resample_alg
        )
        
        new_meta = master_meta.copy()
        new_meta.update({"dtype": 'float32', "nodata": 0, "count": 1})
        with rasterio.open(out_path, "w", **new_meta) as dst:
            dst.write(data_aligned, 1)
    return True

# =================================================================
# 2. PIPELINE CHÍNH
# =================================================================

def main(date_str):
    # Tạo thư mục
    for d in ["Static", "Daily/Rain", "Daily/Soil", "Daily/Tide", "Daily/FloodLabel", "Daily/FloodMask", "Daily/SAR_Denoised"]:
        os.makedirs(os.path.join(OUT_DIR, d), exist_ok=True)
    os.makedirs(TRAIN_DIR, exist_ok=True)

    # Lấy thông tin Master từ DEM
    master_raw_path = os.path.join(BASE_DIR, "Static/Terrain_DEM_Raw.tif")
    if not os.path.exists(master_raw_path):
        logger.error("❌ Cần file DEM gốc để làm chuẩn!")
        return

    with rasterio.open(master_raw_path) as src:
        master_meta = src.meta.copy()
        master_shape = (src.height, src.width)

    # --- Bước 1: Xử lý Static Layers ---
    static_layers = ["Terrain_DEM", "Terrain_Slope", "Terrain_Flow"]
    static_data = {}
    for layer in static_layers:
        in_p = os.path.join(BASE_DIR, f"Static/{layer}_Raw.tif")
        out_p = os.path.join(OUT_DIR, f"Static/{layer}_Proc.tif")
        process_generic_file(in_p, out_p, master_meta, master_shape)
        static_data[layer] = read_raster(out_p)

    # --- Bước 2: Xử lý Daily Layers ---
    # 1. Rain (Lấy 3 ngày: T, T-1, T-2)
    current_dt = datetime.strptime(date_str, "%Y-%m-%d")
    rain_stack = []
    for i in range(3):
        d = (current_dt - timedelta(days=i)).strftime("%Y-%m-%d")
        r_path = os.path.join(BASE_DIR, f"Daily/Rain/Rain_{d}.tif")
        # Nếu thiếu ngày cũ, lấy tạm ngày hiện tại
        if not os.path.exists(r_path): r_path = os.path.join(BASE_DIR, f"Daily/Rain/Rain_{date_str}.tif")
        
        out_r = os.path.join(OUT_DIR, f"Daily/Rain/Rain_{d}.tif")
        process_generic_file(r_path, out_r, master_meta, master_shape)
        rain_stack.append(read_raster(out_r))

    # 2. Soil Moisture
    soil_p = os.path.join(BASE_DIR, f"Daily/Soil/Soil_{date_str}.tif")
    out_soil = os.path.join(OUT_DIR, f"Daily/Soil/Soil_{date_str}.tif")
    process_generic_file(soil_p, out_soil, master_meta, master_shape)
    soil_data = read_raster(out_soil)
    if soil_data is None: soil_data = np.zeros(master_shape, dtype=np.float32)

    # 3. Tide (Constant Layer)
    tide_p = os.path.join(BASE_DIR, f"Daily/Tide/Tide_{date_str}.tif")
    tide_raw = read_raster(tide_p)
    tide_val = np.nanmean(tide_raw) if tide_raw is not None else 0.0
    tide_layer = np.full(master_shape, tide_val, dtype=np.float32)

    # 4. SAR & Labeling
    sar_p = os.path.join(BASE_DIR, f"Daily/FloodLabel/Flood_SAR_{date_str}.tif")
    out_sar = os.path.join(OUT_DIR, f"Daily/FloodLabel/Flood_SAR_{date_str}.tif")
    
    if process_generic_file(sar_p, out_sar, master_meta, master_shape, is_label=True):
        sar_data = read_raster(out_sar)
        # Khử nhiễu
        denoised = lee_filter(sar_data)
        # Tạo Mask vùng có dữ liệu (S1 không phủ kín toàn bộ BBOX mọi lúc)
        v_mask = np.where(sar_data != 0, 1, 0).astype('float32')
        
        # Chỉ tạo sample nếu vùng SAR bao phủ > 5% diện tích
        if (np.sum(v_mask) / v_mask.size) > 0.05:
            # Tạo nhãn mềm (Soft Label Y) dựa trên phân vị 2% và 98%
            valid_vals = denoised[v_mask == 1]
            water_ref, land_ref = np.percentile(valid_vals, 2), np.percentile(valid_vals, 98)
            Y = np.clip((denoised - land_ref) / (water_ref - land_ref + 1e-8), 0, 1) * v_mask

            # --- STACK 9 LAYERS ---
            # 1-3: Rain(T, T-1, T-2), 4: Soil, 5: Tide, 6: DEM, 7: Slope, 8: Flow, 9: SAR_Denoised
            X = np.stack([
                rain_stack[0], rain_stack[1], rain_stack[2],
                soil_data,
                tide_layer,
                static_data["Terrain_DEM"],
                static_data["Terrain_Slope"],
                static_data["Terrain_Flow"],
                denoised
            ], axis=0).astype('float32')

            # Lưu file training
            np.savez_compressed(os.path.join(TRAIN_DIR, f"Sample_{date_str}.npz"), x=X, y=Y)
            logger.info(f"✅ Đã tạo Sample_{date_str}.npz với shape {X.shape}")
        else:
            logger.warning(f"⚠️ Dữ liệu SAR ngày {date_str} quá ít, bỏ qua.")
    else:
        logger.warning(f"⚠️ Không có file SAR cho ngày {date_str}")

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    main(target)