import os
import glob
import rasterio
import numpy as np
from tqdm import tqdm
from datetime import datetime, timedelta
from rasterio.enums import Resampling
from rasterio.warp import reproject

# =================================================================
# 1. CẤU HÌNH HỆ THỐNG
# =================================================================
INPUT_ROOT = "DaNang_Flood_Local" 
NPZ_OUT_DIR = "Data_Training_Raw_NPZ" # Đổi tên để phân biệt với bản Soft Label

MASTER_DEM_FILE = os.path.join(INPUT_ROOT, "Static", "Terrain_DEM_Raw.tif")

os.makedirs(NPZ_OUT_DIR, exist_ok=True)

if not os.path.exists(MASTER_DEM_FILE):
    raise FileNotFoundError(f"❌ Không tìm thấy file Master DEM tại {MASTER_DEM_FILE}")

with rasterio.open(MASTER_DEM_FILE) as src:
    master_meta = src.meta.copy()
    master_shape = (src.height, src.width)
    master_transform = src.transform
    master_crs = src.crs

# =================================================================
# 2. HÀM XỬ LÝ ĐỒNG NHẤT (ALIGNING) TRÊN RAM
# =================================================================
def get_aligned_data(in_path, is_label=False):
    if not os.path.exists(in_path):
        raise FileNotFoundError(f"Missing: {in_path}")
    
    with rasterio.open(in_path) as src:
        data_raw = src.read(1).astype('float32')
        # Chỉ dọn rác cực thấp (ví dụ -32768), giữ lại dải dB thực (thường -35 đến 0)
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
        return data_aligned

# =================================================================
# 3. PIPELINE XỬ LÝ CHÍNH (GIỮ NGUYÊN dB)
# =================================================================
def run_raw_preprocessing(lag_days=2):
    paths = {
        "sar": os.path.join(INPUT_ROOT, "Daily", "FloodLabel"),
        "rain": os.path.join(INPUT_ROOT, "Daily", "Rain"),
        "soil": os.path.join(INPUT_ROOT, "Daily", "Soil"),
        "tide": os.path.join(INPUT_ROOT, "Daily", "Tide"),
        "static": os.path.join(INPUT_ROOT, "Static")
    }

    print("🗺️ Đang chuẩn bị các lớp Static Layers...")
    dem = get_aligned_data(os.path.join(paths["static"], "Terrain_DEM_Raw.tif"))
    slope = get_aligned_data(os.path.join(paths["static"], "Terrain_Slope_Raw.tif"))
    flow = get_aligned_data(os.path.join(paths["static"], "Terrain_Flow_Raw.tif"))

    sar_files = glob.glob(os.path.join(paths["sar"], "*.tif"))
    print(f"📦 Bắt đầu đóng gói {len(sar_files)} mẫu (Nhãn Y = Giá trị dB thực)...")

    for s_path in tqdm(sar_files, desc="Processing"):
        fname = os.path.basename(s_path)
        date_str = fname.replace("Flood_SAR_", "").replace("Label_", "").replace(".tif", "")
        out_npz_path = os.path.join(NPZ_OUT_DIR, f"Sample_{date_str}.npz")

        if os.path.exists(out_npz_path):
            continue

        try:
            current_date = datetime.strptime(date_str, "%Y-%m-%d")

            # --- 1. XỬ LÝ NHÃN Y (GIỮ GIÁ TRỊ dB THỰC) ---
            # Chỉ align để khớp khung hình, không tính toán lại giá trị
            Y = get_aligned_data(s_path, is_label=True)

            # --- 2. THU THẬP DỮ LIỆU ĐẦU VÀO X ---
            rain_series = []
            for i in range(lag_days + 1):
                t_date = current_date - timedelta(days=i)
                r_f = os.path.join(paths["rain"], f"Rain_{t_date.strftime('%Y-%m-%d')}.tif")
                rain_series.append(get_aligned_data(r_f))
            
            soil = get_aligned_data(os.path.join(paths["soil"], f"Soil_{date_str}.tif"))
            tide = get_aligned_data(os.path.join(paths["tide"], f"Tide_{date_str}.tif"))

            # Đóng gói 8-Bands Input (X)
            X = np.stack([*rain_series, soil, tide, dem, slope, flow], axis=0).astype('float32')
            
            # Lưu file NPZ
            np.savez_compressed(out_npz_path, x=X, y=Y)

        except Exception:
            continue

    print(f"✅ Hoàn tất! Nhãn Y trong {NPZ_OUT_DIR} hiện đang lưu giá trị dB thực.")

if __name__ == "__main__":
    run_raw_preprocessing()