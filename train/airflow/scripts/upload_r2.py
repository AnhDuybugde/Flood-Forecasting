import os
import sys
import boto3
import logging
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# --- 📝 CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Upload_R2_Cleanup")

load_dotenv()

# --- 🔑 CẤU HÌNH CLOUDFLARE R2 ---
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "satellite-data")

# Endpoint của R2 dùng cấu trúc S3-compatible
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# --- 📂 CẤU HÌNH ĐƯỜNG DẪN ---
TRAIN_DIR = "/opt/airflow/Data/data_training"

def get_r2_client():
    """Khởi tạo client kết nối R2"""
    return boto3.client(
        service_name="s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto", # R2 yêu cầu region là auto
        config=Config(signature_version="s3v4"),
    )

def upload_and_cleanup(date_str):
    """
    1. Tìm file NPZ tương ứng ngày date_str
    2. Upload lên R2 theo cấu trúc training_data/YYYY-MM/
    3. Xóa file local để tiết kiệm disk
    """
    filename = f"Sample_{date_str}.npz"
    local_path = os.path.join(TRAIN_DIR, filename)
    
    # Định dạng key trên Cloud (vd: training_data/2025-01/Sample_2025-01-01.npz)
    year_month = date_str[:7] 
    r2_key = f"training_data/{year_month}/{filename}"

    # Kiểm tra file local
    if not os.path.exists(local_path):
        # Lưu ý: Một số ngày không có dữ liệu SAR sẽ không có file NPZ
        logger.warning(f"⚠️ Không tìm thấy file: {local_path}. Có thể ngày này không có dữ liệu SAR.")
        # Trả về True để Airflow không báo Task Failure (vì đây là lỗi dữ liệu, không phải lỗi hệ thống)
        return True

    try:
        s3 = get_r2_client()
        
        # Kiểm tra bucket tồn tại (tùy chọn)
        logger.info(f"🚀 Đang tải lên R2: {filename}")
        
        # Thực hiện upload
        s3.upload_file(
            Filename=local_path,
            Bucket=R2_BUCKET_NAME,
            Key=r2_key,
            ExtraArgs={'ContentType': 'application/octet-stream'}
        )
        
        logger.info(f"✅ Upload thành công: {r2_key}")

        # Chỉ xóa file khi chắc chắn upload thành công
        if os.path.exists(local_path):
            os.remove(local_path)
            logger.info(f"🗑️ Đã xóa bản local: {filename}")
        
        return True

    except ClientError as e:
        logger.error(f"❌ Lỗi kết nối R2: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Lỗi hệ thống khi upload ngày {date_str}: {e}")
        return False

if __name__ == "__main__":
    # Nhận {{ ds }} từ Airflow
    if len(sys.argv) >= 2:
        target_date = sys.argv[1]
        success = upload_and_cleanup(target_date)
        if not success:
            sys.exit(1) # Airflow sẽ báo lỗi Task và Retry
    else:
        logger.error("❌ Script yêu cầu tham số ngày (YYYY-MM-DD).")
        sys.exit(1)