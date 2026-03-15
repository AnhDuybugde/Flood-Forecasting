import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# =================================================================
# 1. CẤU HÌNH ĐƯỜNG DẪN (DOCKER ENVIRONMENT)
# =================================================================
# Trong Docker (file docker-compose.yaml), ta đã map thư mục scripts của Windows 
# vào thẳng /opt/airflow/scripts của container. 
# Nên ở đây ta dùng đường dẫn tuyệt đối của Linux luôn cho an toàn.
SCRIPTS_FOLDER = "/opt/airflow/scripts"

# Môi trường Airflow trong Docker đã cấu hình sẵn Python chuẩn
PYTHON_CMD = "python" 

# =================================================================
# 2. CẤU HÌNH DAG
# =================================================================
default_args = {
    'owner': 'anh_duy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='danang_flood_forecasting_etl',
    default_args=default_args,
    description='Full ETL pipeline for Danang flood forecasting',
    schedule_interval='@daily',      
    start_date=datetime(2025, 1, 1), 
    catchup=False,                   
    tags=['flood', 'earth_engine', 'deep_learning'],
) as dag:

    # =================================================================
    # 3. ĐỊNH NGHĨA CÁC TASKS
    # =================================================================
    
    crawl_task = BashOperator(
        task_id='crawl_daily_data',
        # Dùng {{ ds }} để truyền ngày động (vd: 2026-03-15) vào file script
        bash_command=f'{PYTHON_CMD} {SCRIPTS_FOLDER}/crawl.py {{{{ ds }}}} ',
    )

    preprocess_task = BashOperator(
        task_id='preprocess_and_stack_npz',
        bash_command=f'{PYTHON_CMD} {SCRIPTS_FOLDER}/preprocessing.py {{{{ ds }}}} ',
    )

    # 🛑 TẠM TẮT TASK UPLOAD R2 ĐỂ TEST PIPELINE
    # upload_r2_task = BashOperator(
    #     task_id='upload_to_cloudflare_r2',
    #     bash_command=f'{PYTHON_CMD} {SCRIPTS_FOLDER}/upload_r2.py {{{{ ds }}}} ',
    # )

    # =================================================================
    # 4. THIẾT LẬP THỨ TỰ THỰC THI (DEPENDENCIES)
    # =================================================================
    # Chỉ chạy từ Crawl -> Preprocess
    crawl_task >> preprocess_task 
    
    # Khi nào có file upload, đổi lại thành:
    # crawl_task >> preprocess_task >> upload_r2_task