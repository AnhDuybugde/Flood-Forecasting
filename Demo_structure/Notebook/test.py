import os
import glob
import rasterio
import numpy as np

# 📂 ĐƯỜNG DẪN CHUẨN (SỬA CHO ĐÚNG)
INPUT_DIR = r"C:\Users\Administrator\2026\FPT_AIO20A02\DAP391m\Notebook\DaNang_Flood_Local\Daily\FloodLabel"

# 📂 OUTPUT
OUTPUT_DIR = os.path.join(INPUT_DIR, "Flood_Label_Binary")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 🎯 THRESHOLD
THRESHOLD = -15  

# ================================
# 🔍 DEBUG: kiểm tra folder
# ================================
if not os.path.exists(INPUT_DIR):
    print("❌ Sai đường dẫn INPUT_DIR")
    exit()

all_files = os.listdir(INPUT_DIR)
print(f"📂 Tổng file trong folder: {len(all_files)}")

tif_files = glob.glob(os.path.join(INPUT_DIR, "*.tif"))
print(f"🛰️ File .tif tìm được: {len(tif_files)}")

if len(tif_files) == 0:
    print("❌ Không tìm thấy file .tif → kiểm tra lại folder hoặc tên file")
    exit()

# ================================
# 🚀 XỬ LÝ
# ================================
def process_file(input_path):
    filename = os.path.basename(input_path)
    output_path = os.path.join(OUTPUT_DIR, filename)

    with rasterio.open(input_path) as src:
        sar = src.read(1)
        meta = src.meta.copy()

        # xử lý nodata
        if src.nodata is not None:
            sar = np.where(sar == src.nodata, np.nan, sar)

        # 🎯 threshold → flood mask
        flood = (sar < THRESHOLD).astype(np.uint8)

        # update metadata
        meta.update({
            "dtype": "uint8",
            "count": 1,
            "nodata": 0
        })

        with rasterio.open(output_path, "w", **meta) as dst:
            dst.write(flood, 1)

    print(f"✅ Done: {filename}")


# ================================
# 🏁 RUN
# ================================
for f in tif_files:
    process_file(f)

print("🎉 HOÀN THÀNH TOÀN BỘ")