import streamlit as st
import numpy as np
import os
import glob
import folium
from streamlit_folium import st_folium
import matplotlib.pyplot as plt
import pandas as pd
import torch 
import seaborn as sns
from sklearn.metrics import r2_score
from skimage.restoration import denoise_nl_means, estimate_sigma

# Import class mô hình từ file model.py của bạn
from model import FloodSOTAUNet_V2 

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
    """Load dữ liệu cho 1 ngày cụ thể"""
    path = os.path.join(data_dir, f"Sample_{date_str}.npz")
    data = np.load(path)
    return data['x'], data['y']

@st.cache_data(show_spinner="Đang trích xuất dữ liệu tổng quan (Global)...")
def get_global_eda_data(data_dir, dates_list, samples_per_file=5000):
    """Trích xuất ngẫu nhiên dữ liệu từ tất cả các file để làm Global EDA mà không bị tràn RAM"""
    all_features = []
    all_targets = []
    
    # Giới hạn lấy tối đa 20 file để đảm bảo tốc độ nếu dataset quá lớn
    sample_dates = dates_list[:20] 
    
    for d in sample_dates:
        try:
            x, y = load_data(d, data_dir)
            c, h, w = x.shape
            
            # Lấy ngẫu nhiên các index pixel
            indices = np.random.choice(h * w, min(samples_per_file, h*w), replace=False)
            
            # Làm phẳng ma trận và lấy mẫu
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

# ... (Giữ nguyên các hàm calculate_masked_metrics, sota_denoise, load_flood_model, predict_flood như bản trước) ...
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

def sota_denoise(y_pred):
    img = y_pred.astype(np.float64)
    sigma_est = np.mean(estimate_sigma(img, channel_axis=None))
    return denoise_nl_means(img, h=0.8 * sigma_est, fast_mode=True, patch_size=5, patch_distance=6)

@st.cache_resource
def load_flood_model(model_path):
    try:
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        model = FloodSOTAUNet_V2(n_channels=8, n_classes=1)
        state_dict = torch.load(model_path, map_location=device)
        model.load_state_dict(state_dict)
        model.to(device)
        model.eval()
        return model, device
    except Exception as e:
        st.error(f"Lỗi load model: {e}")
        return None, None

def predict_flood(model, device, matrix_x, threshold=0.15):
    x_norm = np.zeros_like(matrix_x, dtype=np.float32)
    for i in range(matrix_x.shape[0]):
        c_min, c_max = matrix_x[i].min(), matrix_x[i].max()
        if c_max > c_min: x_norm[i] = (matrix_x[i] - c_min) / (c_max - c_min)
    input_tensor = torch.from_numpy(x_norm).float().unsqueeze(0).to(device)
    with torch.no_grad():
        logits = model(input_tensor)
        prediction = torch.sigmoid(logits).squeeze().cpu().numpy()
    prediction = sota_denoise(prediction)
    prediction[prediction < threshold] = 0
    if prediction.max() > 0: prediction = (prediction - prediction.min()) / (prediction.max() - prediction.min())
    return np.clip(prediction, 0, 1)

# --- 4. GIAO DIỆN CHÍNH ---
def main():
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "Data")
    MODEL_PATH = os.path.join(BASE_DIR, "best_flood_model_v2.pth")
    
    if not os.path.exists(DATA_DIR):
        st.error(f"⚠️ Thư mục dữ liệu không tồn tại: {DATA_DIR}")
        return
        
    dates = sorted([os.path.basename(f)[7:-4] for f in glob.glob(os.path.join(DATA_DIR, "Sample_*.npz"))])

    # --- SIDEBAR ---
    with st.sidebar:
        st.image("https://cdn-icons-png.flaticon.com/512/2072/2072130.png", width=60)
        st.header("🌊 Flood AI Control")
        if dates:
            selected_date = st.selectbox("📅 Chọn ngày quan trắc", dates)
            X, Y = load_data(selected_date, DATA_DIR)
        else:
            st.warning("Không tìm thấy dữ liệu")
            return

        st.markdown("---")
        opacity = st.slider("Độ trong suốt (Opacity)", 0.0, 1.0, 0.6)

    # --- TABS SYSTEM ---
    tab_map, tab_viz, tab_predict = st.tabs(["🗺️ Interactive Map", "📊 Data Analysis (EDA)", "🤖 AI Prediction"])

    # ==========================================
    # TAB 1: INTERACTIVE MAP (ĐÃ FIX LỖI CLICK)
    # ==========================================
    with tab_map:
        st.subheader("Bản đồ Không gian Đa lớp & Truy vấn Pixel")
        
        # Chia bố cục: Map (75%) - Info Panel (25%)
        col_map, col_info = st.columns([3, 1])
        
        with col_map:
            selected_lyr = st.selectbox("Chọn Layer hiển thị trên bản đồ:", range(8), format_func=lambda x: LAYER_CONFIG[x]["name"])
            m = folium.Map(location=DANANG_CENTER, zoom_start=11, tiles="CartoDB dark_matter")
            
            # Xử lý màu cho Layer Overlay
            img_data = np.nan_to_num(X[selected_lyr])
            cmap = plt.get_cmap(LAYER_CONFIG[selected_lyr]["cmap"])
            norm = plt.Normalize(vmin=img_data.min(), vmax=img_data.max())
            colored_img = cmap(norm(img_data))
            
            # Thêm lớp ảnh và CHỌP SỰ KIỆN CLICK
            folium.raster_layers.ImageOverlay(
                image=colored_img, 
                bounds=DANANG_BOUNDS, 
                opacity=opacity, 
                name=LAYER_CONFIG[selected_lyr]["name"],
                interactive=True # Cho phép tương tác
            ).add_to(m)
            
            # SỬA QUAN TRỌNG: Đổi sang "last_clicked"
            map_output = st_folium(m, width="100%", height=600, returned_objects=["last_clicked"])

        with col_info:
            st.markdown("### 📍 Thông tin Pixel")
            st.caption("Click vào bản đồ để xem dữ liệu 8 Layers tại điểm đó.")
            
            # Bắt sự kiện click
            if map_output and map_output.get("last_clicked"):
                lat = map_output["last_clicked"]["lat"]
                lng = map_output["last_clicked"]["lng"]
                
                # Check xem click có nằm trong khung Đà Nẵng không
                lat_min, lon_min, lat_max, lon_max = 15.95, 107.90, 16.25, 108.40
                
                if lat_min <= lat <= lat_max and lon_min <= lng <= lon_max:
                    # Tính toán Index Ma trận
                    r_idx = int((lat_max - lat) / (lat_max - lat_min) * X.shape[1])
                    c_idx = int((lng - lon_min) / (lon_max - lon_min) * X.shape[2])
                    
                    # Đảm bảo index không vượt quá giới hạn
                    r_idx = min(max(r_idx, 0), X.shape[1] - 1)
                    c_idx = min(max(c_idx, 0), X.shape[2] - 1)
                    
                    st.success(f"**Tọa độ:** {lat:.4f}, {lng:.4f}")
                    
                    # Trình bày dữ liệu đẹp mắt bằng metric
                    st.markdown("---")
                    st.write("**Khí tượng (T, T-1, T-2)**")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Rain (T)", f"{X[0, r_idx, c_idx]:.3f}")
                    c2.metric("Rain (T-1)", f"{X[1, r_idx, c_idx]:.3f}")
                    c3.metric("Rain (T-2)", f"{X[2, r_idx, c_idx]:.3f}")
                    
                    st.write("**Thủy văn & Đất**")
                    c4, c5 = st.columns(2)
                    c4.metric("Flow Acc", f"{X[7, r_idx, c_idx]:.3f}")
                    c5.metric("Soil Moisture", f"{X[3, r_idx, c_idx]:.3f}")
                    
                    st.write("**Địa hình & Triều**")
                    c6, c7, c8 = st.columns(3)
                    c6.metric("DEM", f"{X[5, r_idx, c_idx]:.3f}")
                    c7.metric("Slope", f"{X[6, r_idx, c_idx]:.3f}")
                    c8.metric("Tide", f"{X[4, r_idx, c_idx]:.3f}")
                    
                    st.markdown("---")
                    st.error(f"🎯 **Nhãn Thực Tế (Label Y): {Y[r_idx, c_idx]:.4f}**")
                else:
                    st.warning("⚠️ Bạn đã click ra ngoài phạm vi dữ liệu!")
            else:
                st.info("👆 Hãy click vào một điểm bất kỳ trên bản đồ bên trái.")

    # ==========================================
    # TAB 2: EXPLORATORY DATA ANALYSIS (EDA)
    # ==========================================
    with tab_viz:
        st.subheader("📊 Phân tích Dữ liệu Khám phá")
        
        # Chia 2 phần bằng Sub-tabs
        eda_global, eda_daily = st.tabs(["🌍 TỔNG QUAN (Toàn bộ Dữ liệu)", "📅 CHI TIẾT (Ngày Đang Chọn)"])
        
        # --- PHẦN 1: GLOBAL EDA ---
        with eda_global:
            st.markdown("Phân tích tổng quan dựa trên **lấy mẫu ngẫu nhiên** từ nhiều ngày để đánh giá xu hướng chung của toàn bộ Dataset.")
            df_global = get_global_eda_data(DATA_DIR, dates)
            
            if not df_global.empty:
                g_col1, g_col2 = st.columns([1, 1])
                
                with g_col1:
                    st.markdown("**1. Ma trận Tương quan Tổng thể (Global Correlation)**")
                    fig_gcorr, ax_gcorr = plt.subplots(figsize=(8, 6))
                    sns.heatmap(df_global.corr(), annot=True, cmap='RdBu_r', fmt=".2f", vmin=-1, vmax=1, ax=ax_gcorr)
                    st.pyplot(fig_gcorr)
                    
                with g_col2:
                    st.markdown("**2. So sánh Phân phối các Đặc trưng (Boxplot)**")
                    fig_box, ax_box = plt.subplots(figsize=(8, 6))
                    # Chuẩn hóa min-max để boxplot cùng tỷ lệ dễ nhìn
                    df_norm = (df_global - df_global.min()) / (df_global.max() - df_global.min() + 1e-7)
                    sns.boxplot(data=df_norm.drop(columns=["Target (Y)"]), orient="h", palette="Set2", ax=ax_box)
                    ax_box.set_xlabel("Giá trị chuẩn hóa (0-1)")
                    st.pyplot(fig_box)
            else:
                st.warning("Không đủ dữ liệu để tạo Global EDA.")

        # --- PHẦN 2: DAILY EDA ---
        with eda_daily:
            st.markdown(f"Phân tích chuyên sâu dữ liệu của ngày **{selected_date}**.")
            
            # Sampling cho ngày hiện tại để vẽ nhanh
            c, h, w = X.shape
            sample_size = min(50000, h * w)
            indices = np.random.choice(h * w, sample_size, replace=False)
            df_daily = pd.DataFrame({LAYER_CONFIG[i]["name"]: X[i].flatten()[indices] for i in range(8)})
            df_daily["Target (Y)"] = Y.flatten()[indices]
            
            st.markdown("**1. Mật độ Dữ liệu & Phân phối (KDE Distributions)**")
            fig_dist, axes = plt.subplots(2, 4, figsize=(18, 7))
            plt.subplots_adjust(hspace=0.4, wspace=0.3)
            for i, ax in enumerate(axes.flat):
                sns.histplot(df_daily.iloc[:, i], bins=30, ax=ax, kde=True, color='teal')
                ax.set_title(LAYER_CONFIG[i]["name"], fontsize=11, fontweight='bold')
                ax.set_ylabel("")
            st.pyplot(fig_dist)
            
            d_col1, d_col2 = st.columns(2)
            with d_col1:
                st.markdown("**2. Thống kê Mô tả Nhanh**")
                st.dataframe(df_daily.describe().T[['mean', 'std', 'min', 'max']], use_container_width=True)
            with d_col2:
                # Phân tích độ dốc vs Lượng mưa
                st.markdown("**3. Quan hệ: Mưa (T) và Flow Accumulation**")
                fig_scatter, ax_scatter = plt.subplots(figsize=(6, 4))
                sns.scatterplot(data=df_daily.sample(2000), x="Rain (T)", y="Flow Accumulation", hue="Target (Y)", palette="coolwarm", alpha=0.6, ax=ax_scatter)
                st.pyplot(fig_scatter)

    # ==========================================
    # TAB 3: AI PREDICTION (Giữ nguyên sự hoàn hảo)
    # ==========================================
    with tab_predict:
        st.subheader(f"🤖 Đánh giá Mô hình: FloodSOTA-V2 | Dữ liệu ngày {selected_date}")
        
        if st.button("🚀 Chạy Inference & Trích xuất Metrics", type="primary"):
            model, device = load_flood_model(MODEL_PATH)
            if model:
                with st.spinner("Đang xử lý mạng Nơ-ron và Hậu xử lý NL-Means..."):
                    prediction = predict_flood(model, device, X)
                    
                    flood_cmap = plt.cm.Blues.copy()
                    flood_cmap.set_bad(color='lightgrey') 
                    
                    eval_mask = (Y > 0.001) & (Y < 0.999)
                    Y_disp = np.where(eval_mask, Y, np.nan)
                    Pred_disp = np.where(eval_mask, prediction, np.nan)
                    
                    fig_ai, axes = plt.subplots(1, 3, figsize=(20, 6))
                    
                    axes[0].imshow(X[7], cmap='magma')
                    axes[0].set_title("Flow Accumulation (Địa hình)", fontsize=14)
                    axes[0].axis('off')
                    
                    axes[1].imshow(Y_disp, cmap=flood_cmap, vmin=0, vmax=1)
                    axes[1].set_title("Thực tế (Ground Truth in Eval Zone)", fontsize=14)
                    axes[1].axis('off')
                    
                    im3 = axes[2].imshow(Pred_disp, cmap=flood_cmap, vmin=0, vmax=1)
                    axes[2].set_title("AI Dự báo (Prediction)", fontsize=14)
                    axes[2].axis('off')
                    
                    fig_ai.colorbar(im3, ax=axes, orientation='horizontal', fraction=0.04, pad=0.08, label='Mức độ Ngập')
                    st.pyplot(fig_ai)
                    
                    st.markdown("---")
                    st.markdown("### 📊 Các chỉ số đánh giá Thủy văn (Hydrological Metrics)")
                    mae, r2, nse, kge = calculate_masked_metrics(Y, prediction)
                    
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("NSE (Nash-Sutcliffe)", f"{nse:.4f}")
                    m2.metric("KGE (Kling-Gupta)", f"{kge:.4f}")
                    m3.metric("R² Score", f"{r2:.4f}")
                    m4.metric("MAE", f"{mae:.4f}")

if __name__ == "__main__":
    main()