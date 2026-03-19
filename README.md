# Vietnam Flood Prediction Dashboard

Dashboard dự báo lũ lụt cho Việt Nam được xây dựng từ Google Stitch.

## Cài đặt

```bash
npm install
```

## Chạy ứng dụng

```bash
npm run dev
```

Server sẽ chạy tại: **http://localhost:8000**

## Cấu trúc dự án

```
DAP/
├── public/
│   └── index.html          # Dashboard HTML
├── server.js               # Express server
├── package.json            # Dependencies
└── README.md              # File này
```

## Tính năng

- 🗺️ Bản đồ Việt Nam với lớp dữ liệu lũ lụt
- 📊 Dự báo 7 ngày
- 📈 Biểu đồ lượng mưa và xác suất lũ
- ⚠️ Cảnh báo nguy cơ cao
- 🎛️ Timeline mô phỏng

## Công nghệ

- **Frontend**: HTML, CSS, JavaScript (Vanilla)
- **Backend**: Node.js + Express
- **Charts**: Chart.js
- **Fonts**: Google Fonts (Inter, Outfit)

## Thao tác nhanh (Notes)

**Airflow & Docker:**
```bash
docker compose exec airflow-apiserver airflow dags list
docker compose restart
docker compose down -v
docker compose up -d
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

**Môi trường ảo Python:**
```bash
.\venv\Scripts\activate
```

**Tọa độ tham khảo:**
```
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
