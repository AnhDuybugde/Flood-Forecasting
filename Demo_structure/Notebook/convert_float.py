import os
import rasterio
import logging
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 📝 CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("TifOptimizerMulti")

# --- 📂 CẤU HÌNH ĐƯỜNG DẪN & LUỒNG ---
SOURCE_DIR = "DaNang_Flood_Local_Raw"
TARGET_DIR = "DaNang_Flood_Local_Optimized"

# 💡 MẸO: Đừng để 100 luồng cho tác vụ đọc/ghi ổ cứng. 
# Nếu dùng SSD NVMe: để 8-12. SSD SATA: để 4-6. HDD: để 2-4.
MAX_WORKERS = 8 

def process_single_file(source_path, target_path, file_name):
    """Hàm xử lý độc lập cho 1 file duy nhất để chạy đa luồng"""
    try:
        # Bỏ qua nếu file đích đã tồn tại
        if os.path.exists(target_path):
            return 0, 0, file_name, "Skipped"

        old_size = os.path.getsize(source_path)

        with rasterio.open(source_path) as src:
            meta = src.meta.copy()
            data = src.read()

            # 🛠️ Chuyển đổi sang float32
            if data.dtype in ['float64', 'int64', 'int32']:
                data = data.astype(np.float32)
                meta.update(dtype=rasterio.float32)
            
            # 📦 Bật nén Deflate
            meta.update(compress='deflate', predictor=2, zlevel=6)

            # Ghi file mới
            with rasterio.open(target_path, 'w', **meta) as dst:
                dst.write(data)

        # 🗑️ Kiểm tra và xóa file cũ
        if os.path.exists(target_path) and os.path.getsize(target_path) > 0:
            new_size = os.path.getsize(target_path)
            os.remove(source_path) # Xóa file gốc
            return old_size, new_size, file_name, "Success"
        else:
            return 0, 0, file_name, "Write_Error"

    except Exception as e:
        return 0, 0, file_name, f"Error: {e}"

def run_multithread_optimization():
    if not os.path.exists(SOURCE_DIR):
        logger.error(f"❌ Không tìm thấy thư mục: {SOURCE_DIR}")
        return

    logger.info(f"🚀 Bắt đầu quét và nén ĐA LUỒNG ({MAX_WORKERS} workers)...")
    
    # 1. Thu thập toàn bộ danh sách file cần xử lý
    tasks = []
    for root, _, files in os.walk(SOURCE_DIR):
        for file in files:
            if file.endswith(".tif"):
                source_path = os.path.join(root, file)
                rel_path = os.path.relpath(source_path, SOURCE_DIR)
                target_path = os.path.join(TARGET_DIR, rel_path)
                
                # Tạo trước thư mục đích
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                tasks.append((source_path, target_path, file))

    total_files = len(tasks)
    if total_files == 0:
        logger.info("✅ Không có file .tif nào cần xử lý.")
        return

    logger.info(f"📋 Đã tìm thấy {total_files} files. Đang giao việc cho các luồng...")

    total_saved_bytes = 0
    processed_count = 0

    # 2. Xử lý đa luồng
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit tasks
        futures = {executor.submit(process_single_file, src, tgt, name): name for src, tgt, name in tasks}
        
        # Nhận kết quả
        for future in as_completed(futures):
            processed_count += 1
            old_size, new_size, file_name, status = future.result()
            
            if status == "Success":
                saved = old_size - new_size
                total_saved_bytes += saved
                logger.info(f"[{processed_count}/{total_files}] ✅ Xong: {file_name} | Giảm: {saved/(1024*1024):.1f} MB")
            elif status == "Skipped":
                logger.info(f"[{processed_count}/{total_files}] ⏭️ Bỏ qua (Đã tồn tại): {file_name}")
            else:
                logger.error(f"[{processed_count}/{total_files}] ❌ Thất bại: {file_name} | Lỗi: {status}")

    # 3. Tổng kết
    gb_saved = total_saved_bytes / (1024*1024*1024)
    logger.info("---")
    logger.info(f"✨ HOÀN THÀNH TẤT CẢ! Tổng cộng đã dọn dẹp và tiết kiệm được: {gb_saved:.2f} GB ổ cứng.")

if __name__ == "__main__":
    run_multithread_optimization()