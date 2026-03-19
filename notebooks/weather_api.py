import pandas as pd
import numpy as np
import rasterio
from scipy.interpolate import griddata
from pysheds.grid import Grid
import os

# ==========================================
# 1. CẤU HÌNH ĐƯỜNG DẪN
# ==========================================
PATH_DEM = "DEM_NASADEM_Fixed.tif"
PATH_DATA_CSV = "data_rainfall.csv"    # File có date, location, precipitation...
PATH_STATIONS = "stations_coords.csv" # File có location, longitude, latitude
OUTPUT_FOLDER = "READY_FOR_MODEL"

if not os.path.exists(OUTPUT_FOLDER): os.makedirs(OUTPUT_FOLDER)

# ==========================================
# 2. KIỂM TRA HỆ TỌA ĐỘ (CRS)
# ==========================================
with rasterio.open(PATH_DEM) as src:
    print(f"🌍 DEM CRS: {src.crs}")
    if str(src.crs) != "EPSG:4326":
        print("⚠️ Cảnh báo: DEM không phải EPSG:4326, có thể bị lệch tọa độ!")

# ==========================================
# 3. TÍNH SLOPE & FLOW (DÙNG PYSHEDS & NUMPY)
# ==========================================
def process_terrain(dem_path):
    print("⏳ Đang tính toán địa hình (Slope & Flow)...")
    grid = Grid.from_raster(dem_path)
    dem = grid.read_raster(dem_path)
    
    # Tính Slope bằng Numpy (Nhanh, không lỗi build)
    dy, dx = np.gradient(dem, grid.affine[0], grid.affine[4])
    slope = np.degrees(np.arctan(np.sqrt(dx**2 + dy**2)))
    
    # Tính Flow (Pysheds xử lý tự động)
    pit_filled = grid.fill_pits(dem)
    flooded = grid.fill_depressions(pit_filled)
    inflated = grid.resolve_flats(flooded)
    dirmap = (1, 2, 4, 8, 16, 32, 64, 128)
    fdir = grid.flowdir(inflated, dirmap=dirmap)
    acc = grid.accumulation(fdir)

    # Lưu kết quả
    grid.to_raster(slope, os.path.join(OUTPUT_FOLDER, "Slope.tif"))
    grid.to_raster(np.log1p(acc), os.path.join(OUTPUT_FOLDER, "Flow_Acc_Log.tif")) # Log để thu nhỏ khoảng giá trị
    print("✅ Xong Slope & Flow.")

# ==========================================
# 4. NỘI SUY DỮ LIỆU BẢNG THÀNH ẢNH
# ==========================================
def table_to_raster(data_csv, stations_csv, ref_dem, features):
    df_data = pd.read_csv(data_csv)
    df_stations = pd.read_csv(stations_csv)
    
    # Gộp tọa độ vào dữ liệu (Nếu CSV của bạn chưa có lat/lon)
    df = pd.merge(df_data, df_stations, on='location', how='left')
    
    with rasterio.open(ref_dem) as src:
        grid_shape = src.shape
        res_profile = src.profile
        rows, cols = np.indices(grid_shape)
        lon_grid, lat_grid = rasterio.transform.xy(src.transform, rows, cols)
        lon_grid, lat_grid = np.array(lon_grid), np.array(lat_grid)

    for col in features:
        print(f"⏳ Đang tạo ảnh cho: {col}")
        points = df[['longitude', 'latitude']].dropna().values
        values = df[col].dropna().values
        
        # Nội suy từ điểm trạm đo ra toàn bộ grid của DEM
        grid_data = griddata(points, values, (lon_grid, lat_grid), method='linear')
        grid_data = np.nan_to_num(grid_data, nan=0)

        res_profile.update(dtype=rasterio.float32, count=1)
        with rasterio.open(os.path.join(OUTPUT_FOLDER, f"Image_{col}.tif"), 'w', **res_profile) as dst:
            dst.write(grid_data.astype(rasterio.float32), 1)
    
# ==========================================
# CHẠY TOÀN BỘ
# ==========================================
try:
    process_terrain(PATH_DEM)
    
    cols = ['precipitation_sum', 'humidity_mean', 'soil_moisture_0_7cm', 'soil_moisture_7_28cm']
    table_to_raster(PATH_DATA_CSV, PATH_STATIONS, PATH_DEM, cols)
    
    print("\n🎉 Pipeline hoàn tất! Tất cả ảnh đã sẵn sàng trong thư mục READY_FOR_MODEL.")
except Exception as e:
    print(f"❌ Lỗi: {e}")
import pandas as pd
import numpy as np
import rasterio
from scipy.interpolate import griddata
from pysheds.grid import Grid
import os

# ==========================================
# 1. CẤU HÌNH ĐƯỜNG DẪN
# ==========================================
PATH_DEM = "DEM_NASADEM_Fixed.tif"
PATH_DATA_CSV = "data_rainfall.csv"    # File có date, location, precipitation...
PATH_STATIONS = "stations_coords.csv" # File có location, longitude, latitude
OUTPUT_FOLDER = "READY_FOR_MODEL"

if not os.path.exists(OUTPUT_FOLDER): os.makedirs(OUTPUT_FOLDER)

# ==========================================
# 2. KIỂM TRA HỆ TỌA ĐỘ (CRS)
# ==========================================
with rasterio.open(PATH_DEM) as src:
    print(f"🌍 DEM CRS: {src.crs}")
    if str(src.crs) != "EPSG:4326":
        print("⚠️ Cảnh báo: DEM không phải EPSG:4326, có thể bị lệch tọa độ!")

# ==========================================
# 3. TÍNH SLOPE & FLOW (DÙNG PYSHEDS & NUMPY)
# ==========================================
def process_terrain(dem_path):
    print("⏳ Đang tính toán địa hình (Slope & Flow)...")
    grid = Grid.from_raster(dem_path)
    dem = grid.read_raster(dem_path)
    
    # Tính Slope bằng Numpy (Nhanh, không lỗi build)
    dy, dx = np.gradient(dem, grid.affine[0], grid.affine[4])
    slope = np.degrees(np.arctan(np.sqrt(dx**2 + dy**2)))
    
    # Tính Flow (Pysheds xử lý tự động)
    pit_filled = grid.fill_pits(dem)
    flooded = grid.fill_depressions(pit_filled)
    inflated = grid.resolve_flats(flooded)
    dirmap = (1, 2, 4, 8, 16, 32, 64, 128)
    fdir = grid.flowdir(inflated, dirmap=dirmap)
    acc = grid.accumulation(fdir)

    # Lưu kết quả
    grid.to_raster(slope, os.path.join(OUTPUT_FOLDER, "Slope.tif"))
    grid.to_raster(np.log1p(acc), os.path.join(OUTPUT_FOLDER, "Flow_Acc_Log.tif")) # Log để thu nhỏ khoảng giá trị
    print("✅ Xong Slope & Flow.")

# ==========================================
# 4. NỘI SUY DỮ LIỆU BẢNG THÀNH ẢNH
# ==========================================
def table_to_raster(data_csv, stations_csv, ref_dem, features):
    df_data = pd.read_csv(data_csv)
    df_stations = pd.read_csv(stations_csv)
    
    # Gộp tọa độ vào dữ liệu (Nếu CSV của bạn chưa có lat/lon)
    df = pd.merge(df_data, df_stations, on='location', how='left')
    
    with rasterio.open(ref_dem) as src:
        grid_shape = src.shape
        res_profile = src.profile
        rows, cols = np.indices(grid_shape)
        lon_grid, lat_grid = rasterio.transform.xy(src.transform, rows, cols)
        lon_grid, lat_grid = np.array(lon_grid), np.array(lat_grid)

    for col in features:
        print(f"⏳ Đang tạo ảnh cho: {col}")
        points = df[['longitude', 'latitude']].dropna().values
        values = df[col].dropna().values
        
        # Nội suy từ điểm trạm đo ra toàn bộ grid của DEM
        grid_data = griddata(points, values, (lon_grid, lat_grid), method='linear')
        grid_data = np.nan_to_num(grid_data, nan=0)

        res_profile.update(dtype=rasterio.float32, count=1)
        with rasterio.open(os.path.join(OUTPUT_FOLDER, f"Image_{col}.tif"), 'w', **res_profile) as dst:
            dst.write(grid_data.astype(rasterio.float32), 1)
    
# ==========================================
# CHẠY TOÀN BỘ
# ==========================================
try:
    process_terrain(PATH_DEM)
    
    cols = ['precipitation_sum', 'humidity_mean', 'soil_moisture_0_7cm', 'soil_moisture_7_28cm']
    table_to_raster(PATH_DATA_CSV, PATH_STATIONS, PATH_DEM, cols)
    
    print("\n🎉 Pipeline hoàn tất! Tất cả ảnh đã sẵn sàng trong thư mục READY_FOR_MODEL.")
except Exception as e:
    print(f"❌ Lỗi: {e}")