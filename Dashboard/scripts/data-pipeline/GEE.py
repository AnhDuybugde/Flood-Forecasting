def run():
    print("🚀 Step 1 chạy")
    import ee
    import geemap
    import os

    # INIT
    ee.Initialize(
        ee.ServiceAccountCredentials(
            "gee-airflow@landsurface-485908.iam.gserviceaccount.com",
            "/opt/airflow/secrets/gee-airflow.json"
        ),
        project="landsurface-485908"
    )

    print("EE READY")


    BASE_DIR = "/opt/airflow/data/FloodForecastingDataset"
    SCALE = 250  # ~250–500m (OK cho flood ML)

    # REGIONS
    regions = {
        "DBSCL": ee.Geometry.Rectangle([104.4, 8.5, 106.8, 11.0]),
        "CentralCoast": ee.Geometry.Rectangle([107.4, 13.5, 109.5, 16.5])
    }

    # DATE RANGE
    start_date = ee.Date("2025-01-01")
    end_date   = ee.Date("2025-12-31")
    n_days = end_date.difference(start_date, "day").getInfo()

    # DATASETS
    DEM  = ee.Image("USGS/SRTMGL1_003")
    FLOW = ee.Image("WWF/HydroSHEDS/15ACC").select("b1")
    LANDCOVER = ee.Image("ESA/WorldCover/v100/2020").select("Map")

    RAIN_IC = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
    SOIL_IC = ee.ImageCollection("NASA/SMAP/SPL4SMGP/007")

    # Permanent water mask
    PERM_WATER = (
        ee.Image("JRC/GSW1_4/GlobalSurfaceWater")
        .select("occurrence")
        .gt(90)
    )

    # FUNCTION: DAILY FLOOD LABEL (Sentinel-1)
    def sentinel1_flood_mask(region, start, end):
        s1 = (
            ee.ImageCollection("COPERNICUS/S1_GRD")
            .filterBounds(region)
            .filterDate(start, end)
            .filter(ee.Filter.eq("instrumentMode", "IW"))
            .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
            .filter(ee.Filter.eq("orbitProperties_pass", "DESCENDING"))
            .select("VV")
        )

        if s1.size().getInfo() == 0:
            return None

        img = s1.mean()

        # Speckle reduction
        img = img.focal_mean(radius=50, units="meters")

        flood = (
            img.lt(-15)
            .And(PERM_WATER.Not())
        )

        return flood.rename("FloodMask")

    # FUNCTION: STATIC FLOOD SUSCEPTIBILITY (PRIOR)
    def flood_susceptibility(region):
        dem = DEM.clip(region)
        flow = FLOW.clip(region)

        dem_norm  = dem.unitScale(-5, 50)
        flow_norm = flow.log10().unitScale(0, 5)

        flood_sus = (
            dem_norm.multiply(0.4)
            .add(flow_norm.multiply(0.6))
        )

        return flood_sus.rename("FloodSusceptibility")

    # MAIN LOOP
    for region_name, region in regions.items():
        print(f"\n🚀 Processing region: {region_name}")

        REGION_DIR = os.path.join(BASE_DIR, region_name)
        STATIC_DIR = os.path.join(REGION_DIR, "Static")
        DAILY_DIR  = os.path.join(REGION_DIR, "Daily")
        LABEL_DIR  = os.path.join(REGION_DIR, "LabelDaily")

        os.makedirs(STATIC_DIR, exist_ok=True)
        os.makedirs(DAILY_DIR, exist_ok=True)
        os.makedirs(LABEL_DIR, exist_ok=True)

        # 1. STATIC FEATURES
        dem = DEM.clip(region).unmask(0)
        slope = ee.Terrain.slope(dem)
        flow = FLOW.clip(region).add(1).log10().unitScale(0, 5)
        lc = LANDCOVER.clip(region)

        geemap.ee_export_image(dem,   f"{STATIC_DIR}/DEM.tif", scale=SCALE, region=region)
        geemap.ee_export_image(slope, f"{STATIC_DIR}/Slope.tif", scale=SCALE, region=region)
        geemap.ee_export_image(flow,  f"{STATIC_DIR}/FlowAccumulation.tif", scale=SCALE, region=region)
        geemap.ee_export_image(lc,    f"{STATIC_DIR}/LandCover.tif", scale=SCALE, region=region)

        print("✅ Static features saved")

        # 2. STATIC FLOOD PRIOR
        flood_static = flood_susceptibility(region)

        geemap.ee_export_image(
            flood_static,
            f"{STATIC_DIR}/FloodSusceptibility.tif",
            scale=SCALE,
            region=region
        )

        print("✅ Static Flood Susceptibility saved")

        # 3. DAILY FEATURES + LABEL
        for d in range(n_days):
            day = start_date.advance(d, "day")
            day_str = day.format("YYYY_MM_dd").getInfo()

            # ---------- RAIN ----------
            rain_col = RAIN_IC.filterBounds(region).filterDate(day, day.advance(1, "day"))
            if rain_col.size().getInfo() > 0:
                rain = rain_col.sum().clip(region).unitScale(0, 200)
                geemap.ee_export_image(
                    rain,
                    f"{DAILY_DIR}/Rain_{day_str}.tif",
                    scale=SCALE,
                    region=region
                )

            # ---------- SOIL MOISTURE ----------
            soil_col = (
                SOIL_IC
                .filterBounds(region)
                .filterDate(day, day.advance(1, "day"))
                .select("sm_surface")
            )

            if soil_col.size().getInfo() > 0:
                soil = soil_col.mean().clip(region)

                soil = soil.setDefaultProjection(
                    crs="EPSG:4326",
                    scale=10000
                )

                soil = soil.reduceResolution(
                    reducer=ee.Reducer.mean(),
                    maxPixels=1024
                ).reproject(
                    crs="EPSG:4326",
                    scale=SCALE
                )

                soil = soil.unitScale(0, 0.5)

                geemap.ee_export_image(
                    soil,
                    f"{DAILY_DIR}/SoilMoisture_{day_str}.tif",
                    scale=SCALE,
                    region=region
                )

            # ---------- DAILY FLOOD LABEL ----------
            flood = sentinel1_flood_mask(
                region,
                day,
                day.advance(1, "day")
            )

            if flood is None:
                print(f"⚠️ No Sentinel-1 data {day_str}")
                continue

            geemap.ee_export_image(
                flood,
                f"{LABEL_DIR}/Flood_{day_str}.tif",
                scale=SCALE,
                region=region
            )

            print(f"✅ Saved DAILY data + label {day_str}")

    print("\n✨ DONE: Full Flood Forecasting Dataset (Static + Daily + Labels)")
