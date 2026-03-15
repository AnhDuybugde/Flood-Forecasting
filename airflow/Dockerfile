FROM apache/airflow:2.8.2-python3.11

USER root

# Giữ nguyên các phần cài đặt hệ thống bên dưới
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgdal-dev \
    g++ \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường để Rasterio tìm thấy thư viện C++
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

USER airflow

# Nâng cấp pip trước khi cài requirements
RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt