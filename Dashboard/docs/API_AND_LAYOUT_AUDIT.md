# API & Layout Audit Report — Vietnam Flood Prediction System

**Date:** 2025-03-16  
**Scope:** Frame misalignment, API correctness, region/date sync

---

## Issues Found & Fixed

### 1. **7-Day Forecast region label — FIXED**
- **Problem:** Panel hiển thị "Ho Chi Minh City" trong khi bản đồ là khu vực Đà Nẵng.
- **Root cause:** Hardcoded label trong HTML, không lấy theo region hiện tại.
- **Fix:** 
  - Thêm `id="forecast-region-label"` với mặc định "Đà Nẵng".
  - Trong `updateHeatmap()` gọi `document.getElementById('forecast-region-label')` và cập nhật theo `window.currentRegion`.

### 2. **Không đồng bộ `window.currentDate` / `window.currentRegion` — FIXED**
- **Problem:** `data-panels.js` và `date-manager.js` dùng `window.currentDate` / `window.currentRegion` nhưng `map.js` không gán giá trị vào `window`.
- **Fix:**
  - Trong `updateHeatmap()` thêm `window.currentDate = date` và `window.currentRegion = region`.
  - Trong `date-manager.js` khi nhận latest date: luôn set `window.currentDate` và `window.currentRegion`.

### 3. **7-Day Forecast nội dung tĩnh — LƯU Ý**
- **Status:** Phần nội dung (Oct 12, 13, 14, 15, LOW/MEDIUM/HIGH) là HTML tĩnh, không gọi API.
- **Suggestion:** Cần API dự báo 7 ngày (forecast) để thay thế dữ liệu mẫu.

### 4. **Rainfall Trend Chart**
- **API:** `GET /api/forecast/:region/rainfall-trend?date=YYYY-MM-DD` — gọi đúng region và date.
- **Logic:** `updateTimelineExternal()` luôn gọi `updateRainfallTrend()` khi ngày thay đổi (kể cả khi không có trong timeline).
- **Nếu biểu đồ trống:** Kiểm tra API trả về `envelope.success` và `envelope.data`; có thể không có dữ liệu cho ngày chọn.

---

## API Route Reference (Verified)

| Endpoint | Params | Purpose |
|----------|--------|---------|
| `GET /api/dates/:region` | region=DaNang | Danh sách ngày có dữ liệu |
| `GET /api/timeline` | — | Timeline (hiện chỉ dùng DaNang) |
| `GET /api/pixel/:lat/:lng/:date/:region` | YYYY-MM-DD | Dữ liệu pixel |
| `GET /api/mask/:region/:date/label.png` | YYYY-MM-DD | Flood mask PNG |
| `GET /api/grid/:region/:date/:layer?format=bin` | layer=label, rain, ... | Grid binary |
| `GET /api/forecast/:region/rainfall-trend?date=` | YYYY-MM-DD | Xu hướng mưa 7 ngày |
| `GET /api/forecast/:region/history?startDate=&endDate=` | YYYY-MM-DD | Lịch sử region |
| `GET /api/v1/flood-risk?region=&date=` | query | GeoJSON flood risk |
| `GET /api/heatmap/:region/:date/:layer` | — | Metadata cho heatmap |
| `POST /api/inference/predict` | body | ML prediction |

---

## Bounds & Region Consistency

| Component | Bounds/Source |
|-----------|---------------|
| `REGION_BOUNDS` (map.js, common.ts) | DaNang: north 16.25, south 15.95, east 108.40, west 107.90 |
| `MASK_BOUNDS` (map.js) | DaNang: [[15.95, 107.90], [16.25, 108.40]] (Leaflet [south, west], [north, east]) |
| `RegionSchema` | Chỉ hỗ trợ `'DaNang'` |

---

## Date Flow

1. **Calendar / Manual input** → `selectUnifiedDate(dateStr)`:
   - `updateHeatmap(dateStr, region)` → map, window.currentDate, region label
   - `updateTimeline(dateStr)` → timeline slider (nếu có trong `timelineDates`), rainfall trend
2. **Timeline slider** → `handleTimelineSlider()`:
   - `updateHeatmap(date, region)`, `updateRainfallTrend(date, region)`
3. **Map init** → `api/dates/DaNang` → lấy ngày mới nhất → `updateHeatmap()` → sync `window.currentDate`

---

## Remaining Recommendations

1. **7-Day Forecast API:** Triển khai endpoint thực để thay thế nội dung tĩnh.
2. **Timeline theo region:** `/api/timeline` hiện chỉ lấy DaNang; nếu có thêm region, cần tham số region (ví dụ `?region=DaNang`).
3. **Ngày vượt quá dữ liệu:** Chọn ngày không có dữ liệu (vd. 31/12/2025) có thể gây chart trống; cần xử lý lỗi rõ ràng ở frontend.
