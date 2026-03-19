import os
import pandas as pd
import numpy as np
import rasterio
import requests
from scipy.spatial import cKDTree
from datetime import datetime, timedelta
import glob

# =================================================================
# 1. CẤU HÌNH ĐƯỜNG DẪN & THÔNG SỐ
# =================================================================
TRAIN_NPZ_DIR = "Data"
RAW_LOG = "Data_Inference_Raw"
TIF_DIR = "Data_Inference_Tif"
NPZ_DIR = "Data_Inference_Final"

for folder in [RAW_LOG, TIF_DIR, NPZ_DIR]:
    os.makedirs(folder, exist_ok=True)

MASTER_SHAPE = (1115, 1856)
STATIONS = {
    "Cam_Le": {"lat": 16.02, "lon": 108.20},
    "Hoa_Vang": {"lat": 16.00, "lon": 108.05},
    "Lien_Chieu": {"lat": 16.08, "lon": 108.15},
    "Son_Tra": {"lat": 16.12, "lon": 108.25},
    "Ba_Na": {"lat": 15.99, "lon": 107.99}
}

# =================================================================
# 2. CHUẨN BỊ DỮ LIỆU TĨNH (COPY TỪ SAMPLE TRAIN)
# =================================================================
def get_static_layers_from_sample():
    all_samples = glob.glob(os.path.join(TRAIN_NPZ_DIR, "*.npz"))
    if not all_samples:
        raise FileNotFoundError(f"❌ Không thấy file mẫu nào trong {TRAIN_NPZ_DIR}")
    
    sample_path = all_samples[0]
    print(f"📦 Trích xuất lớp tĩnh (DEM, Slope, Flow) từ Sample: {os.path.basename(sample_path)}")
    
    with np.load(sample_path) as data:
        x_full = data['x'] 
        return x_full[5], x_full[6], x_full[7]

# Load 1 lần dùng mãi mãi
STATIC_DEM, STATIC_SLOPE, STATIC_FLOW = get_static_layers_from_sample()

# =================================================================
# 3. HÀM XỬ LÝ CHÍNH THEO NGÀY
# =================================================================
def process_inference_day(date_str, is_flood_test=False):
    print(f"\n--- 📅 Đang xử lý: {date_str} {'(CHẾ ĐỘ ĐỐI CHỨNG LŨ)' if is_flood_test else ''} ---")
    
    # --- STEP 1: LẤY DỮ LIỆU (API HOẶC BENCHMARK) ---
    api_results = []
    API_KEY = "f253639864aa4d6b6fbfdf5306116d86"

    for name, coords in STATIONS.items():
        if is_flood_test:
            # GIẢ LẬP DỮ LIỆU MƯA LỚN (Benchmark thực tế Đà Nẵng)
            rain_val = np.random.uniform(250.0, 450.0) # mm/ngày
            hum_val = np.random.uniform(95, 100)
            pres_val = np.random.uniform(990, 1005) # Áp suất thấp khi bão
        else:
            # GỌI API THỰC TẾ 2026
            try:
                url = f"https://api.openweathermap.org/data/2.5/weather?lat={coords['lat']}&lon={coords['lon']}&appid={API_KEY}&units=metric"
                res = requests.get(url).json()
                rain_val = res.get('rain', {}).get('1h', 0)
                hum_val = res['main']['humidity']
                pres_val = res['main']['pressure']
            except:
                rain_val, hum_val, pres_val = 0, 70, 1012 # Mặc định ngày nắng

        api_results.append({
            "station": name, "lat": coords['lat'], "lon": coords['lon'],
            "rain": rain_val, "humidity": hum_val, "pressure": pres_val,
            "soil_proxy": hum_val / 100.0,
            "tide_proxy": (pres_val - 1000) / 10.0
        })
    
    # Lưu CSV
    df = pd.DataFrame(api_results)
    df.to_csv(os.path.join(RAW_LOG, f"Weather_Raw_{date_str}.csv"), index=False)

    # --- STEP 2: NỘI SUY IDW ---
    coords_arr = df[['lon', 'lat']].values
    lons_lin = np.linspace(107.9, 108.3, MASTER_SHAPE[1])
    lats_lin = np.linspace(15.9, 16.2, MASTER_SHAPE[0])
    lon_grid, lat_grid = np.meshgrid(lons_lin, lats_lin)
    grid_points = np.column_stack([lon_grid.ravel(), lat_grid.ravel()])
    
    tree = cKDTree(coords_arr)
    dist, idx = tree.query(grid_points, k=len(STATIONS))
    weights = 1.0 / (np.maximum(dist, 1e-9)**2)

    def interp(col):
        v = df[col].values
        return (np.sum(weights * v[idx], axis=1) / np.sum(weights, axis=1)).reshape(MASTER_SHAPE)

    rain_t, soil, tide = interp('rain'), interp('soil_proxy'), interp('tide_proxy')

    # Lưu TIF để dùng làm quá khứ (T-1, T-2) cho ngày tiếp theo
    for img, n in zip([rain_t, soil, tide], ["Rain", "Soil", "Tide"]):
        with rasterio.open(os.path.join(TIF_DIR, f"{n}_{date_str}.tif"), 'w', driver='GTiff', 
                           height=MASTER_SHAPE[0], width=MASTER_SHAPE[1], count=1, 
                           dtype='float32', crs='EPSG:4326', 
                           transform=rasterio.transform.from_origin(107.9, 16.2, 0.0002, 0.0002)) as dst:
            dst.write(img.astype('float32'), 1)

    # --- STEP 3: ĐÓNG GÓI NPZ 8 LỚP ---
    t_obj = datetime.strptime(date_str, "%Y-%m-%d")
    def get_past_rain(days):
        d_str = (t_obj - timedelta(days=days)).strftime("%Y-%m-%d")
        path = os.path.join(TIF_DIR, f"Rain_{d_str}.tif")
        if os.path.exists(path):
            with rasterio.open(path) as s: return s.read(1)
        return np.zeros(MASTER_SHAPE, dtype='float32')

    rain_t1, rain_t2 = get_past_rain(1), get_past_rain(2)

    # GHÉP LẠI (5 Lớp mới + 3 Lớp tĩnh COPY từ Train Sample)
    X = np.stack([rain_t, rain_t1, rain_t2, soil, tide, STATIC_DEM, STATIC_SLOPE, STATIC_FLOW], axis=0)
    
    out_npz = os.path.join(NPZ_DIR, f"Inference_Input_{date_str}.npz")
    np.savez_compressed(out_npz, x=X.astype('float32'))
    print(f"🚀 Thành công: {out_npz}")

# =================================================================
# 4. CHẠY PIPELINE
# =================================================================
if __name__ == "__main__":
    # 1. Chạy các ngày đối chứng (Flood Benchmark) để test Model
    flood_days = ["2026-01-06", "2026-02-15","2026-03-05","2026-03-19","2025-10-27"]
    for d in flood_days:
        process_inference_day(d, is_flood_test=True)

    # 2. Chạy ngày hiện tại 2026 (Thực tế nắng ráo)
    current_days = ["2026-03-18", "2026-03-19"]
    for d in current_days:
        process_inference_day(d, is_flood_test=False)

    print("\n✅ TẤT CẢ FILE ĐÃ SẴN SÀNG TRONG THƯ MỤC Data_Inference_Final!")