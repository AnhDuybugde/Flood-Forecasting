# 🌊 Da Nang Flood AI Prediction System

Welcome to the **Da Nang Flood AI Prediction System**! This project is an advanced flood forecasting ecosystem that combines satellite data, deep learning (ResNet101 ONNX), and interactive web dashboards to predict flood risks in Da Nang, Vietnam.

## 🌟 Key Features

1. **Interactive Web Dashboard** (`Dashboard/frontend/`):
   - A modern HTML/JS interface featuring interactive Leaflet maps.
   - Click on any pixel on the map to see a *Quick Overview* of Rain, Soil Moisture, and Risk.
   - Comprehensive **Pixel EDA** (Exploratory Data Analysis) with detailed metrics (Tide, DEM, Slope) and insightful charts (Distribution, Correlation, Seasonality) using Chart.js.

2. **AI Inference & Data Visualization** (`Demo_structure/app.py`):
   - A versatile Streamlit application that evaluates the AI model's accuracy.
   - Extracts live weather data from OpenWeatherMap to create simulated flood risk previews.
   - Super-fast inferencing powered by ONNX Runtime.

3. **Automated Data Pipeline** (`airflow/` & Python Scripts):
   - Automated fetching of satellite inputs (Rainfall, Soil Moisture via Earth Engine) and Tide levels (Copernicus).
   - Designed to upload processed data to cloud storage seamlessly.

## 🏗️ Project Structure

```text
📁 flood_predict_project/
├── 📁 Dashboard/              
│   ├── 📁 frontend/         # Core HTML/JS/CSS web interface (Dashboard & EDA charts)
│   └── 📄 app.py            # Main application/scripts for the dashboard backend
├── 📁 Demo_structure/         # Streamlit App & Cloud Data inference experiments
├── 📁 airflow/                # Airflow DAGs for automated data collection
└── 📄 best_flood_model.pth  # Weights of the trained Flood prediction model
```

## 🛠️ How to Run

### 1. View the Web Dashboard
Since the frontend is built using standard web technologies:
1. Open the `Dashboard/frontend` directory.
2. Run a local development server (e.g., using VSCode's **Live Server** extension).
3. Open `index.html` to view the interactive map, and explore `detail.html` for comprehensive Exploratory Data Analysis.

### 2. Run the Streamlit AI App
If you wish to test the live inference or model metrics:
```bash
# Activate your python virtual environment
.\venv\Scripts\activate

# Install dependencies if you haven't already
pip install -r requirements.txt

# Run the Streamlit app
cd Demo_structure
streamlit run app.py
```

## 💡 Notes on Cloud Data
- The interactive dashboards and Streamlit applications rely on actual environmental parameters.
- Some scripts utilize the OpenWeatherMap API and Google Earth Engine to securely extract the latest data. Please ensure you configure your API Keys correctly in the environment files if you decide to redeploy the backend!
