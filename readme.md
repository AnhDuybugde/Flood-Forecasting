# 🌊 Da Nang Flood AI Prediction System (DAP391m)

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-Node.js-339933.svg)](https://nodejs.org/)
[![Orchestration](https://img.shields.io/badge/Workflow-Airflow-017CEE.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Container-2496ED.svg)](https://www.docker.com/)

Hệ thống tích hợp thu thập dữ liệu tự động (**Airflow**), phân tích dữ liệu (**EDA**) và dự báo ngập lụt khu vực Đà Nẵng bằng mô hình Deep Learning (**FloodSOTA-V2**).

---

## 🌟 Tính Năng Nổi Bật (Key Features)

1. **Interactive Web Dashboard** (`Dashboard/`):
   - Giao diện Web hiện đại (Frontend Node.js/HTML/Vanilla JS) với bản đồ Leaflet trực quan.
   - **Tích hợp Live Cloud API**: Trực tiếp kéo dữ liệu lượng mưa từ OpenWeatherMap 5 ngày tới.
   - Click vào một điểm bất kỳ để xem Pop-up tóm tắt nhanh: Lượng mưa, Độ ẩm đất, Cảnh báo ngập.
   - Trang phân tích sâu **Advanced Pixel EDA** bao gồm các biểu đồ đa dạng (Seasonality, Feature Distribution, Correlation, Time Series) được thiết kế qua Chart.js.

2. **AI Inference & Data Visualization (Streamlit)** (`Demo_structure/`):
   - Ứng dụng độc lập phục vụ cho việc kiểm thử độ chính xác của AI.
   - Ứng dụng inference siêu tốc qua thư viện ONNX Runtime.

3. **Automated Data Pipeline** (`airflow/` & Python Scripts):
   - Hệ thống Pipeline tự động lấy ảnh vệ tinh lượng mưa, độ ẩm qua Google Earth Engine và lấy thủy triều từ Copernicus Marine Service.

---

## 📂 Dữ liệu & Mô hình (Data & Model)

Do kích thước tệp rất lớn, một phần dữ liệu và trọng số mô hình được lưu trữ riêng biệt trên Cloud. Vui lòng tải xuống theo hướng dẫn sau:

> 📌 **[Tải dữ liệu và Model gốc tại đây](https://drive.google.com/drive/folders/1BrEy_5SQVTIG97ofJbNlTW4fuC2SkgDv?usp=drive_link)**

| Thành phần | Định dạng | Vị trí lưu trữ sau khi tải |
| :--- | :--- | :--- |
| **Dữ liệu huấn luyện** | `.npz` | Thư mục `Data/` (Root) hoặc `Demo_structure/Data_Training/` |
| **Trọng số Model** | `.pth` / `.onnx` | Thư mục gốc project / `Demo_structure/` |

---

## 🏗️ Cấu trúc dự án (Project Structure)

```text
DAP/
├── 📁 airflow/                # Quản lý DAGs và Auto-Pipeline thu thập ảnh vệ tinh
├── 📁 Dashboard/              # API Backend (Node/Express) và Interactive Web
│   ├── 📁 frontend/         # Dashboard HTML, Vanilla CSS, JS (Chart.js)
│   └── 📁 src/              # Backend TypeScript, Controllers, Routers
├── 📁 Demo_structure/         # Source code UI Streamlit test tính năng AI
│   └── 📄 app.py            # Streamlit Dashboard cũ
├── 📁 Data/                   # Chứa data huấn luyện .npz (Git ignored)
├── 📄 best_flood_model_v2.pth # File trọng số AI model chuẩn gốc
├── 📄 docker-compose.yml      # Cấu hình Docker để scale hệ thống backend/pipeline
└── 📄 README.md               # File này
```

---

## 🛠️ Hướng dẫn Khởi chạy (Setup & Execution)

### 1. Interactive Web Dashboard (Node.js/HTML)
Để trải nghiệm Dashboard giao diện Web với API thực từ OpenWeather:
```bash
# Vào thư mục Dashboard
cd Dashboard

# Cài đặt package thư viện Node
npm install

# Khởi chạy Server Backend API cùng tĩnh phục vụ Frontend
npm run dev
```
> 👉 *Lưu ý:* Mở địa chỉ `http://localhost:8001` (hoặc cổng hiển thị trong Terminal) trên trình duyệt để thấy các biểu đồ EDA mới nhất. Không dùng chức năng *Live Server* tích hợp của VSCode.

### 2. Streamlit AI App (Old Dashboard)
Phục vụ việc chạy Prediction thủ công và EDA cục bộ:
```bash
# Kích hoạt môi trường
.\venv\Scripts\activate

# Cài đặt Package cho môi trường Python
pip install -r requirements.txt

# Khởi động app
cd Demo_structure
streamlit run app.py
```

### 3. Hệ thống Pipeline (Airflow qua Docker)
Vận hành hệ thống thu thập tự động theo chu kỳ mỗi ngày:
```bash
docker compose down -v 
docker compose up -d --build
```
> 🌐 Vào trang Quản lý DAG: `http://localhost:8080`

---

## 💡 Ghi Chú & Tọa độ Tham Khảo

**Thao tác nhanh Airflow & Docker:**
```bash
docker compose exec airflow-apiserver airflow dags list
docker compose restart
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

**Tọa độ tham khảo các trạm (Review Coordinate Boundaries):**
```text
🌊 Duyên hải miền Trung (BẮC → NAM)
Thanh Hóa (Sầm Sơn)      19.75   105.90
Nghệ An (Vinh / Hòn Ngư) 18.68   105.68
Quảng Trị (Cồn Cỏ)       17.16   107.33
Đà Nẵng (Sơn Trà)        16.05   108.20
Bình Định (Quy Nhơn)     13.77   109.23
Bình Thuận (Phú Quý)     10.50   108.97

🌾 Đồng bằng sông Cửu Long
Cần Thơ                  10.03   105.78
An Giang (Long Xuyên)    10.38   105.44
Đồng Tháp (Cao Lãnh)     10.46   105.63
Cà Mau                   9.18    105.15
Kiên Giang (Phú Quốc)    10.23   103.96
```

_Các file Python backend trực tiếp truy suất data thông qua API (Google Earth Engine, OpenWeatherMap, Copernicus). Khi deploy lên Production thực tế, xin hãy cấu hình biến môi trường AWS/R2 đầy đủ._
