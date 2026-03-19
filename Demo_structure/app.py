import os
import glob
import folium
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from streamlit_folium import st_folium
from sklearn.metrics import r2_score
from scipy.ndimage import gaussian_filter 
from scipy.special import expit # Dùng cho hàm Sigmoid của ONNX
import onnxruntime as ort # Thư viện chạy ONNX
import rasterio
import requests
from scipy.spatial import cKDTree
from datetime import datetime, timedelta
from skimage.restoration import denoise_nl_means, estimate_sigma 


# --- 1. CẤU HÌNH TRANG ---
st.set_page_config(page_title="Da Nang Flood AI Pro", layout="wide", page_icon="🌊")

DANANG_CENTER = [16.10, 108.15]
DANANG_BOUNDS = [[15.95, 107.90], [16.25, 108.40]]

LAYER_CONFIG = {
    0: {"name": "Rain (T)", "cmap": "Blues"},
    1: {"name": "Rain (T-1)", "cmap": "Blues"},
    2: {"name": "Rain (T-2)", "cmap": "Blues"},
    3: {"name": "Soil Moisture", "cmap": "YlGn"},
    4: {"name": "Tide Level", "cmap": "PuBu"},
    5: {"name": "DEM (Elevation)", "cmap": "terrain"},
    6: {"name": "Slope", "cmap": "copper"},
    7: {"name": "Flow Accumulation", "cmap": "YlGnBu"}
}

# --- 2. HÀM CORE & XỬ LÝ DỮ LIỆU ---
@st.cache_data
def load_data(date_str, data_dir):
    path = os.path.join(data_dir, f"Sample_{date_str}.npz")
    if not os.path.exists(path):
        return None, None
    data = np.load(path)
    return data['x'], data['y']

@st.cache_data(show_spinner="Đang trích xuất dữ liệu tổng quan (Global)...")
def get_global_eda_data(data_dir, actual_dates_list, samples_per_file=5000):
    all_features = []
    all_targets = []
    sample_dates = actual_dates_list[:20] 
    
    for d in sample_dates:
        try:
            x, y = load_data(d, data_dir)
            if x is None: continue
            c, h, w = x.shape
            indices = np.random.choice(h * w, min(samples_per_file, h*w), replace=False)
            x_flat = x.reshape(c, -1)[:, indices]
            y_flat = y.flatten()[indices]
            all_features.append(x_flat)
            all_targets.append(y_flat)
        except Exception:
            continue
            
    if not all_features:
        return pd.DataFrame()
        
    global_x = np.concatenate(all_features, axis=1)
    global_y = np.concatenate(all_targets)
    df = pd.DataFrame({LAYER_CONFIG[i]["name"]: global_x[i] for i in range(8)})
    df["Target (Y)"] = global_y
    return df

def calculate_masked_metrics(y_true, y_pred):
    mask = (y_true > 0.001) & (y_true < 0.999) 
    if not np.any(mask): return 0, 0, 0, 0
    y_t, y_p = y_true[mask], y_pred[mask]
    mae = np.mean(np.abs(y_t - y_p))
    num = np.sum((y_t - y_p)**2)
    den = np.sum((y_t - np.mean(y_t))**2)
    nse = 1 - (num / (den + 1e-10))
    r2 = r2_score(y_t, y_p)
    if np.std(y_t) == 0: kge = 0
    else:
        r = np.corrcoef(y_t, y_p)[0, 1] if np.std(y_p) > 0 else 0
        alpha = np.std(y_p) / (np.std(y_t) + 1e-10)
        beta = np.mean(y_p) / (np.mean(y_t) + 1e-10)
        kge = 1 - np.sqrt((r-1)**2 + (alpha-1)**2 + (beta-1)**2)
    return mae, r2, nse, kge

def fast_denoise(y_pred):
    return gaussian_filter(y_pred, sigma=1.0)

def sota_denoise_optimized(y_pred):
    img = y_pred.astype(np.float64)
    sigma_est = np.mean(estimate_sigma(img, channel_axis=None))
    return denoise_nl_means(img, h=0.8 * sigma_est, fast_mode=True, patch_size=3, patch_distance=5)

@st.cache_resource
def load_flood_model(model_path):
    """Load ONNX Model và kiểm tra CUDA GPU"""
    try:
        # Cấu hình ưu tiên CUDA, dự phòng CPU
        providers = ['CUDAExecutionProvider', 'CPUExecutionProvider']
        session = ort.InferenceSession(model_path, providers=providers)
        
        # Kiểm tra xem ONNX Runtime có thực sự nhận được CUDA không
        active_provider = session.get_providers()[0]
        
        input_name = session.get_inputs()[0].name
        return session, input_name, active_provider
    except Exception as e:
        st.error(f"Lỗi load model ONNX: {e}")
        return None, None, None

def predict_flood(session, input_name, matrix_x, threshold=0.01, fast_eval=False): 
    # --- CHUẨN HÓA SIÊU TỐC (VECTORIZED NUMPY) ---
    c_min = matrix_x.reshape(8, -1).min(axis=1).reshape(8, 1, 1)
    c_max = matrix_x.reshape(8, -1).max(axis=1).reshape(8, 1, 1)
    
    # Chỉ chia khi c_max > c_min, tránh chia cho 0
    valid_mask = (c_max > c_min)
    x_norm = np.zeros_like(matrix_x, dtype=np.float32)
    np.divide((matrix_x - c_min), (c_max - c_min), out=x_norm, where=valid_mask)
            
    input_data = np.expand_dims(x_norm, axis=0)
    
    # --- INFERENCE ---
    logits = session.run(None, {input_name: input_data})[0]
    prediction = expit(logits).squeeze()
        
    # --- KHỬ NHIỄU ---
    if fast_eval:
        prediction = fast_denoise(prediction) 
    else:
        prediction = sota_denoise_optimized(prediction)
    
    # --- FIX LỖI NODATA: Hạ Threshold và giữ nguyên base 0 ---
    prediction[prediction < threshold] = 0
    if prediction.max() > 0: 
        prediction = prediction / prediction.max()
        
    return np.clip(prediction, 0, 1)

@st.fragment 
def display_interactive_prediction_map(prediction, key_suffix):
    if prediction.max() == 0:
        st.info("🟢 Dữ liệu ngày này mô hình dự báo KHÔNG CÓ NGẬP (toàn bộ pixel = 0).")
        return 
    
    m = folium.Map(location=DANANG_CENTER, zoom_start=11, tiles="CartoDB dark_matter")
    cmap = plt.cm.Blues
    norm = plt.Normalize(vmin=0, vmax=1)
    colored_img = cmap(norm(prediction))
    colored_img[prediction == 0, 3] = 0 
    
    folium.raster_layers.ImageOverlay(
        image=colored_img, bounds=DANANG_BOUNDS, opacity=0.7, 
        name="AI Flood Prediction", interactive=True
    ).add_to(m)
    
    map_output = st_folium(m, width="100%", height=500, returned_objects=["last_clicked"], key=f"pred_map_{key_suffix}")
    
    if map_output and map_output.get("last_clicked"):
        lat = map_output["last_clicked"]["lat"]
        lng = map_output["last_clicked"]["lng"]
        
        lat_min, lon_min, lat_max, lon_max = 15.95, 107.90, 16.25, 108.40
        if lat_min <= lat <= lat_max and lon_min <= lng <= lon_max:
            r_idx = int((lat_max - lat) / (lat_max - lat_min) * prediction.shape[0])
            c_idx = int((lng - lon_min) / (lon_max - lon_min) * prediction.shape[1])
            r_idx = min(max(r_idx, 0), prediction.shape[0] - 1)
            c_idx = min(max(c_idx, 0), prediction.shape[1] - 1)
            
            val = prediction[r_idx, c_idx]
            if val > 0:
                st.error(f"📍 Tọa độ: {lat:.4f}, {lng:.4f} | 🌊 **Xác suất ngập: {val:.4f}**")
            else:
                st.success(f"📍 Tọa độ: {lat:.4f}, {lng:.4f} | 🟢 **Vùng an toàn (Không ngập)**")
        else:
            st.warning("⚠️ Bạn đã click ra ngoài phạm vi Đà Nẵng!")

# --- 4. GIAO DIỆN CHÍNH ---
def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_VIZ_DIR = os.path.join(BASE_DIR, "Data_Visualize")
    DATA_TRAIN_DIR = os.path.join(BASE_DIR, "Data_Training")
    MODEL_PATH = os.path.join(BASE_DIR, "flood_resnet101_web_int8.onnx")

    if not os.path.exists(DATA_VIZ_DIR) or not os.path.exists(DATA_TRAIN_DIR):
        st.error(f"⚠️ Thiếu thư mục dữ liệu! Vui lòng đảm bảo có đủ thư mục:\n- {DATA_VIZ_DIR}\n- {DATA_TRAIN_DIR}")
        return
        
    all_train_files = sorted(glob.glob(os.path.join(DATA_TRAIN_DIR, "Sample_*.npz")))
    if not all_train_files:
        st.warning("Không tìm thấy dữ liệu trong Data_Training")
        return

    actual_dates = [os.path.basename(f)[7:-4] for f in all_train_files]
    total_files = len(actual_dates)
    train_end = int(total_files * 0.8)
    val_end = int(total_files * 0.9)
    
    display_dates = []
    for i, date_str in enumerate(actual_dates):
        if i < train_end:
            display_dates.append(f"🔴 [TRAIN] {date_str}")
        elif i < val_end:
            display_dates.append(f"🟡 [VAL] {date_str}")
        else:
            display_dates.append(f"🟢 [TEST] {date_str}")

    # Khởi tạo model ở cấp cao để check GPU
    session, input_name, provider = load_flood_model(MODEL_PATH)

    # --- SIDEBAR ---
    with st.sidebar:
        st.image("https://cdn-icons-png.flaticon.com/512/2072/2072130.png", width=60)
        st.header("🌊 Flood AI Control")
        
        # Báo cáo tình trạng GPU
        if provider == "CUDAExecutionProvider":
            st.success("🟢 CUDA GPU: Đang Hoạt Động (Siêu tốc)")
        else:
            st.warning("🟠 CPU: Đang chạy bằng CPU (ONNX không tìm thấy CUDA)")
            
        st.markdown("---")
        
        selected_display = st.selectbox("📅 Chọn ngày quan trắc", display_dates)
        idx = display_dates.index(selected_display)
        selected_date = actual_dates[idx]
        
        X_viz, Y_viz = load_data(selected_date, DATA_VIZ_DIR)
        X_train, Y_train = load_data(selected_date, DATA_TRAIN_DIR)
        
        if X_viz is None or X_train is None:
            st.error(f"Lỗi: Dữ liệu ngày {selected_date} bị thiếu ở Data_Visualize hoặc Data_Training!")
            st.stop()
        
        if "TEST" in selected_display:
            st.info("✅ Có thể chạy AI Prediction.")
        else:
            st.warning("⚠️ Tính năng AI Prediction bị khóa.")

        st.markdown("---")
        opacity = st.slider("Độ trong suốt Bản đồ", 0.0, 1.0, 0.6)

    # --- TABS SYSTEM ---
    tab_map, tab_viz, tab_predict, tab_live = st.tabs([
        "🗺️ Interactive Map", "📊 Data Analysis", "🤖 AI Prediction", "🌐 Live Data"
    ])

    # ==========================================
    # TAB 1: INTERACTIVE MAP
    # ==========================================
    with tab_map:
        st.subheader("Bản đồ Không gian Đa lớp & Truy vấn Pixel")
        col_map, col_info = st.columns([3, 1])
        
        with col_map:
            selected_lyr = st.selectbox("Chọn Layer hiển thị trên bản đồ:", range(8), format_func=lambda x: LAYER_CONFIG[x]["name"])
            m = folium.Map(location=DANANG_CENTER, zoom_start=11, tiles="CartoDB dark_matter")
            
            img_data = np.nan_to_num(X_viz[selected_lyr])
            cmap = plt.get_cmap(LAYER_CONFIG[selected_lyr]["cmap"])
            norm = plt.Normalize(vmin=img_data.min(), vmax=img_data.max())
            colored_img = cmap(norm(img_data))
            
            folium.raster_layers.ImageOverlay(
                image=colored_img, bounds=DANANG_BOUNDS, opacity=opacity, 
                name=LAYER_CONFIG[selected_lyr]["name"], interactive=True
            ).add_to(m)
            
            map_output = st_folium(m, width="100%", height=600, returned_objects=["last_clicked"])

        with col_info:
            st.markdown("### 📍 Thông tin Pixel")
            st.caption("Click vào bản đồ để xem dữ liệu.")
            if map_output and map_output.get("last_clicked"):
                lat = map_output["last_clicked"]["lat"]
                lng = map_output["last_clicked"]["lng"]
                lat_min, lon_min, lat_max, lon_max = 15.95, 107.90, 16.25, 108.40
                
                if lat_min <= lat <= lat_max and lon_min <= lng <= lon_max:
                    r_idx = int((lat_max - lat) / (lat_max - lat_min) * X_viz.shape[1])
                    c_idx = int((lng - lon_min) / (lon_max - lon_min) * X_viz.shape[2])
                    r_idx = min(max(r_idx, 0), X_viz.shape[1] - 1)
                    c_idx = min(max(c_idx, 0), X_viz.shape[2] - 1)
                    
                    st.success(f"**Tọa độ:** {lat:.4f}, {lng:.4f}")
                    st.markdown("---")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Rain (T)", f"{X_viz[0, r_idx, c_idx]:.3f}")
                    c2.metric("Rain (T-1)", f"{X_viz[1, r_idx, c_idx]:.3f}")
                    c3.metric("Rain (T-2)", f"{X_viz[2, r_idx, c_idx]:.3f}")
                    
                    c4, c5 = st.columns(2)
                    c4.metric("Flow Acc", f"{X_viz[7, r_idx, c_idx]:.3f}")
                    c5.metric("Soil Moisture", f"{X_viz[3, r_idx, c_idx]:.3f}")
                    
                    c6, c7, c8 = st.columns(3)
                    c6.metric("DEM", f"{X_viz[5, r_idx, c_idx]:.3f}")
                    c7.metric("Slope", f"{X_viz[6, r_idx, c_idx]:.3f}")
                    c8.metric("Tide", f"{X_viz[4, r_idx, c_idx]:.3f}")
                    
                    st.markdown("---")
                    st.error(f"🎯 **Nhãn Thực Tế (Label Y): {Y_viz[r_idx, c_idx]:.4f}**")
                else:
                    st.warning("⚠️ Bạn đã click ra ngoài phạm vi dữ liệu!")

    # ==========================================
    # TAB 2: EXPLORATORY DATA ANALYSIS
    # ==========================================
    with tab_viz:
        st.subheader("📊 Phân tích Dữ liệu")
        eda_global, eda_daily = st.tabs(["🌍 TỔNG QUAN", "📅 CHI TIẾT"])
        
        with eda_global:
            df_global = get_global_eda_data(DATA_VIZ_DIR, actual_dates)
            if not df_global.empty:
                g_col1, g_col2 = st.columns([1, 1])
                with g_col1:
                    st.markdown("**1. Ma trận Tương quan**")
                    fig_gcorr, ax_gcorr = plt.subplots(figsize=(8, 6))
                    sns.heatmap(df_global.corr(), annot=True, cmap='RdBu_r', fmt=".2f", vmin=-1, vmax=1, ax=ax_gcorr)
                    st.pyplot(fig_gcorr)
                with g_col2:
                    st.markdown("**2. Phân phối (Boxplot)**")
                    fig_box, ax_box = plt.subplots(figsize=(8, 6))
                    df_norm = (df_global - df_global.min()) / (df_global.max() - df_global.min() + 1e-7)
                    sns.boxplot(data=df_norm.drop(columns=["Target (Y)"]), orient="h", palette="Set2", ax=ax_box)
                    st.pyplot(fig_box)

        with eda_daily:
            c, h, w = X_viz.shape
            indices = np.random.choice(h * w, min(50000, h * w), replace=False)
            df_daily = pd.DataFrame({LAYER_CONFIG[i]["name"]: X_viz[i].flatten()[indices] for i in range(8)})
            df_daily["Target (Y)"] = Y_viz.flatten()[indices]
            
            fig_dist, axes = plt.subplots(2, 4, figsize=(18, 7))
            plt.subplots_adjust(hspace=0.4, wspace=0.3)
            for i, ax in enumerate(axes.flat):
                sns.histplot(df_daily.iloc[:, i], bins=30, ax=ax, kde=True, color='teal')
                ax.set_title(LAYER_CONFIG[i]["name"], fontsize=11, fontweight='bold')
            st.pyplot(fig_dist)
            
            d_col1, d_col2 = st.columns(2)
            with d_col1:
                st.dataframe(df_daily.describe().T[['mean', 'std', 'min', 'max']], use_container_width=True)
            with d_col2:
                fig_scatter, ax_scatter = plt.subplots(figsize=(6, 4))
                sns.scatterplot(data=df_daily.sample(2000), x="Rain (T)", y="Flow Accumulation", hue="Target (Y)", palette="coolwarm", alpha=0.6, ax=ax_scatter)
                st.pyplot(fig_scatter)

    # ==========================================
    # TAB 3: AI PREDICTION (ONNX)
    # ==========================================
    with tab_predict:
        st.subheader("🤖 Đánh giá Mô hình: ONNX Runtime (Quantized INT8)")
        tab_daily_pred, tab_global_eval = st.tabs(["📅 Đánh giá theo ngày", "🌍 Đánh giá Tổng thể"])
        
        with tab_daily_pred:
            if "TEST" not in selected_display:
                st.error("🚫 Chọn ngày **🟢 [TEST]** ở Sidebar để đánh giá.")
            else:
                if st.button("🚀 Chạy Inference Ngày Này", type="primary"):
                    if session:
                        with st.spinner("Đang chạy Mạng Nơ-ron siêu tốc qua ONNX..."):
                            prediction = predict_flood(session, input_name, X_train, fast_eval=False)
                            st.session_state['current_prediction'] = prediction
                
                if 'current_prediction' in st.session_state:
                    pred = st.session_state['current_prediction']
                    st.markdown("### 🗺️ Bản đồ Tương tác")
                    display_interactive_prediction_map(pred, key_suffix="tab3")
                    
                    st.markdown("---")
                    st.markdown("### 📊 Các chỉ số đánh giá Thủy văn")
                    mae, r2, nse, kge = calculate_masked_metrics(Y_train, pred)
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("NSE", f"{nse:.4f}")
                    m2.metric("KGE", f"{kge:.4f}")
                    m3.metric("R² Score", f"{r2:.4f}")
                    m4.metric("MAE", f"{mae:.4f}")
        
        with tab_global_eval:
            if st.button("🔄 Bắt đầu chạy Global Evaluation"):
                test_files = [f for f in all_train_files if actual_dates[all_train_files.index(f)] > actual_dates[val_end]]
                if not test_files:
                    st.warning("Không tìm thấy file Test nào!")
                else:
                    results = []
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for idx, file_path in enumerate(test_files):
                        file_name = os.path.basename(file_path)
                        status_text.text(f"Đang xử lý {idx+1}/{len(test_files)}: {file_name}")
                        try:
                            date_str = file_name[7:-4]
                            x_t, y_t = load_data(date_str, DATA_TRAIN_DIR)
                            y_p = predict_flood(session, input_name, x_t, fast_eval=True)
                            
                            mae, r2, nse, kge = calculate_masked_metrics(y_t, y_p)
                            results.append({'file': file_name, 'NSE': nse, 'KGE': kge, 'MAE': mae, 'R2': r2})
                        except Exception as e:
                            pass
                        progress_bar.progress((idx + 1) / len(test_files))
                    
                    status_text.text("✅ Hoàn tất!")
                    st.session_state['global_eval_df'] = pd.DataFrame(results)
            
            if 'global_eval_df' in st.session_state:
                df = st.session_state['global_eval_df']
                fig, axes = plt.subplots(1, 2, figsize=(18, 6))
                sns.histplot(df['NSE'], bins=15, kde=True, ax=axes[0], color='royalblue')
                axes[0].axvline(df['NSE'].mean(), color='red', linestyle='--')
                axes[0].set_title('Phân bổ NSE')
                sns.boxplot(data=df[['NSE', 'KGE']], ax=axes[1], palette="Set2")
                axes[1].set_title('Độ biến thiên NSE và KGE')
                st.pyplot(fig)
                
                col_top, col_bot = st.columns(2)
                with col_top:
                    st.success("🏆 TOP 5 FILE TỐT NHẤT")
                    st.dataframe(df.sort_values('NSE', ascending=False)[['file', 'NSE', 'KGE']].head(5), hide_index=True)
                with col_bot:
                    st.error("🚨 BOTTOM 5 FILE")
                    st.dataframe(df.sort_values('NSE', ascending=True)[['file', 'NSE', 'KGE']].head(5), hide_index=True)

    # ==========================================
    # TAB 4: LIVE API & INFERENCE (ONNX)
    # ==========================================
    with tab_live:
        st.subheader("📡 Cập nhật thời tiết & Dự báo Real-time")
        
        STATIONS = {
            "Cam_Le": {"lat": 16.02, "lon": 108.20}, "Hoa_Vang": {"lat": 16.00, "lon": 108.05},
            "Lien_Chieu": {"lat": 16.08, "lon": 108.15}, "Son_Tra": {"lat": 16.12, "lon": 108.25},
            "Ba_Na": {"lat": 15.99, "lon": 107.99}
        }
        API_KEY = "f253639864aa4d6b6fbfdf5306116d86"

        col_input, col_action = st.columns([2, 1])
        with col_input:
            target_date = st.date_input("📅 Chọn ngày", datetime.today())
            simulation_mode = st.radio("Chế độ:", ["🌤️ API Thực tế", "⛈️ Giả lập Mưa bão"])
        with col_action:
            st.write("\n\n")
            run_live = st.button("⚡ Chạy Dự Báo", type="primary", use_container_width=True)

        if run_live:
            date_str = target_date.strftime("%Y-%m-%d")
            c, h, w = X_train.shape
            master_shape = (h, w)

            with st.spinner("📦 Đang trích xuất dữ liệu địa hình tĩnh..."):
                sample_file = all_train_files[0] 
                with np.load(sample_file) as data:
                    STATIC_DEM, STATIC_SLOPE, STATIC_FLOW = data['x'][5], data['x'][6], data['x'][7]

            with st.spinner("🌩️ Đang gọi API thời tiết & Nội suy IDW..."):
                api_results = []
                is_flood_test = "Giả lập" in simulation_mode
                for name, coords in STATIONS.items():
                    if is_flood_test:
                        rain_val, hum_val, pres_val = np.random.uniform(250.0, 450.0), np.random.uniform(95, 100), np.random.uniform(990, 1005)
                    else:
                        try:
                            url = f"https://api.openweathermap.org/data/2.5/weather?lat={coords['lat']}&lon={coords['lon']}&appid={API_KEY}&units=metric"
                            res = requests.get(url).json()
                            rain_val, hum_val, pres_val = res.get('rain', {}).get('1h', 0) * 24, res['main']['humidity'], res['main']['pressure']
                        except:
                            rain_val, hum_val, pres_val = 0, 70, 1012
                    api_results.append({"lon": coords['lon'], "lat": coords['lat'], "rain": rain_val, "soil": hum_val/100.0, "tide": (pres_val-1000)/10.0})

                df_live = pd.DataFrame(api_results)
                lons_lin, lats_lin = np.linspace(107.9, 108.3, master_shape[1]), np.linspace(15.9, 16.2, master_shape[0])
                lon_grid, lat_grid = np.meshgrid(lons_lin, lats_lin)
                grid_points = np.column_stack([lon_grid.ravel(), lat_grid.ravel()])
                tree = cKDTree(df_live[['lon', 'lat']].values)
                dist, idx = tree.query(grid_points, k=len(STATIONS))
                weights = 1.0 / (np.maximum(dist, 1e-9)**2)

                def interp(col):
                    return (np.sum(weights * df_live[col].values[idx], axis=1) / np.sum(weights, axis=1)).reshape(master_shape)

                rain_t, soil, tide = interp('rain'), interp('soil'), interp('tide')
                X_live = np.stack([rain_t, np.zeros(master_shape), np.zeros(master_shape), soil, tide, STATIC_DEM, STATIC_SLOPE, STATIC_FLOW], axis=0)

            with st.spinner("🤖 Đang Inference qua ONNX..."):
                if session:
                    live_pred = predict_flood(session, input_name, X_live, fast_eval=False)
                    st.session_state['live_prediction'], st.session_state['live_rain_t'] = live_pred, rain_t
                    st.success("✅ Hoàn tất!")

        if 'live_prediction' in st.session_state:
            fig_live, axes_live = plt.subplots(1, 2, figsize=(14, 5))
            axes_live[0].imshow(st.session_state['live_rain_t'], cmap='Blues')
            axes_live[0].set_title("Lượng mưa nội suy")
            axes_live[1].imshow(st.session_state['live_prediction'], cmap=plt.cm.Blues, vmin=0, vmax=1)
            axes_live[1].set_title("Bản đồ ngập tĩnh")
            st.pyplot(fig_live)
            
            st.markdown("### 🗺️ BẢN ĐỒ DỰ BÁO TƯƠNG TÁC")
            display_interactive_prediction_map(st.session_state['live_prediction'], key_suffix="tab4")

if __name__ == "__main__":
    main()