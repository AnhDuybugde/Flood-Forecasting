import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# =================================================================
# 1. CẤU HÌNH ĐƯỜNG DẪN (DOCKER ENVIRONMENT)
# =================================================================
SCRIPTS_FOLDER = "/opt/airflow/scripts"
# Tốt nhất là sử dụng đường dẫn tuyệt đối đến python của venv nếu có, 
# hoặc giữ nguyên "python" nếu bạn cài thư viện vào thẳng môi trường container.
PYTHON_CMD = "python" 

# =================================================================
# 2. CẤU HÌNH DAG
# =================================================================
default_args = {
    'owner': 'anh_duy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,               # Tăng lên 2 lần vì cào dữ liệu vệ tinh hay bị timeout
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='danang_flood_forecasting_etl',
    default_args=default_args,
    description='Full ETL pipeline for Danang flood forecasting',
    schedule_interval='0 2 * * *',   # Chạy vào 2:00 sáng mỗi ngày (để chờ dữ liệu vệ tinh cập nhật)
    start_date=datetime(2025, 12, 1), 
    catchup=True,                    # Chuyển thành True nếu bạn muốn Airflow tự động cào bù dữ liệu từ quá khứ
    tags=['flood', 'earth_engine', 'deep_learning'],
    max_active_runs=1,               # Quan trọng: Tránh nhiều ngày chạy cùng lúc gây nghẽn RAM khi xử lý ảnh raster
) as dag:

    # =================================================================
    # 3. ĐỊNH NGHĨA CÁC TASKS
    # =================================================================
    
    # Task 1: Cào dữ liệu
    crawl_task = BashOperator(
        task_id='crawl_daily_data',
        # Thêm biến môi trường nếu cần thiết ngay trong lệnh bash
        bash_command=f'{PYTHON_CMD} {SCRIPTS_FOLDER}/crawl.py "{{{{ ds }}}}"',
    )

    # Task 2: Tiền xử lý (Align, Denoise, Stack 9 layers)
    preprocess_task = BashOperator(
        task_id='preprocess_and_stack_npz',
        bash_command=f'{PYTHON_CMD} {SCRIPTS_FOLDER}/preprocessing.py "{{{{ ds }}}}"',
    )

    # Task 3: Upload lên R2 và dọn dẹp local
    upload_r2_task = BashOperator(
        task_id='upload_to_cloudflare_r2',
        bash_command=f'{PYTHON_CMD} {SCRIPTS_FOLDER}/upload_r2.py "{{{{ ds }}}}"',
    )

    # =================================================================
    # 4. THIẾT LẬP THỨ TỰ THỰC THI (DEPENDENCIES)
    # =================================================================
    crawl_task >> preprocess_task >> upload_r2_task