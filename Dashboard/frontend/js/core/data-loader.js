/**
 * data-loader.js
 * Module quản lý load & cache dữ liệu từ API
 *
 * Mọi response từ backend đều theo envelope: { success, data, error }
 * Module này unwrap và trả về chỉ phần `data` cho phần code còn lại.
 */

class DataLoader {
    constructor() {
        this.cache = new Map();
        this.maxCacheSize = 200; // Needs to hold: 6 years of seasonality + timeline + pixel data + ML
        this.currentRegion = 'DaNang';
        this.availableDates = null;  // { availableDates, totalDays, dateRange, ... }
    }

    // ----------------------------------------------------------
    // PRIVATE: Fetch helper với envelope unwrapping
    // ----------------------------------------------------------

    /**
     * Gọi API và unwrap envelope { success, data, error }.
     * Throw lỗi nếu !success, trả về data nếu thành công.
     * Supports timeout via AbortController (default: 120s).
     */
    async _fetch(url, timeoutMs = 120000) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);

        try {
            const response = await fetch(url, { signal: controller.signal });
            const envelope = await response.json();

            if (!envelope.success) {
                const errMsg = envelope.error?.message || `HTTP ${response.status}`;
                throw new Error(errMsg);
            }
            return envelope.data;
        } catch (err) {
            if (err.name === 'AbortError') {
                throw new Error(`Request timed out after ${timeoutMs}ms: ${url}`);
            }
            throw err;
        } finally {
            clearTimeout(timer);
        }
    }

    // ----------------------------------------------------------
    // Cache helpers
    // ----------------------------------------------------------

    _cacheGet(key) { return this.cache.get(key) || null; }

    _cacheSet(key, value) {
        if (this.cache.size >= this.maxCacheSize) {
            this.cache.delete(this.cache.keys().next().value);
        }
        this.cache.set(key, value);
    }

    // ----------------------------------------------------------
    // PUBLIC API
    // ----------------------------------------------------------

    /**
     * Load danh sách ngày có dữ liệu cho một region.
     * @returns { region, dateRange, totalDays, availableDates, dataSources }
     */
    async loadAvailableDates(region) {
        const cacheKey = `dates_${region}`;
        const cached = this._cacheGet(cacheKey);
        if (cached) return cached;

        try {
            const data = await this._fetch(`${window.API_BASE_URL}/api/dates/${region}`);

            this._cacheSet(cacheKey, data);

            // Lưu lại cho region mặc định
            if (region === this.currentRegion) {
                this.availableDates = data;
            }

            console.log(`✅ Loaded ${data.totalDays} available dates for ${region}`);

            document.dispatchEvent(new CustomEvent('dataLoaded', { detail: { region, data } }));
            return data;
        } catch (error) {
            console.error(`❌ Error loading available dates for ${region}:`, error.message);
            return null;
        }
    }

    /**
     * Kiểm tra ngày có dữ liệu không (dựa trên cache dates của currentRegion)
     */
    isDateAvailable(dateStr) {
        if (!this.availableDates?.availableDates) return false;
        const [year, month, day] = dateStr.split('-');
        return this.availableDates.availableDates?.[year]?.[month]?.includes(parseInt(day)) || false;
    }

    /**
     * Load dữ liệu pixel tại toạ độ & ngày cụ thể.
     * @returns { lat, lng, date, region, rainfall, soilMoisture, tide, flood, floodRisk, ... }
     */
    async loadPixelData(lat, lng, date, region) {
        const cacheKey = `pixel_${region}_${date}_${lat}_${lng}`;
        const cached = this._cacheGet(cacheKey);
        if (cached) return cached;

        try {
            const url = `${window.API_BASE_URL}/api/pixel/${lat}/${lng}/${date}/${region}`;
            const data = await this._fetch(url);
            this._cacheSet(cacheKey, data);
            console.log(`✅ Pixel [${lat}, ${lng}] on ${date}: flood=${data.flood}, rain=${data.rainfall}`);
            return data;
        } catch (error) {
            console.warn(`⚠️ No pixel data at [${lat},${lng}] on ${date}: ${error.message}`);
            return null;
        }
    }

    /**
     * Load heatmap metadata (bounds + maskUrl) cho 1 layer.
     * Frontend dùng maskUrl để render PNG overlay lên Leaflet.
     * @returns { layer, date, region, bounds, maskUrl }
     */
    async loadHeatmapData(region, date, layer = 'rain') {
        const cacheKey = `heatmap_${region}_${date}_${layer}`;
        const cached = this._cacheGet(cacheKey);
        if (cached) return cached;

        try {
            const url = `${window.API_BASE_URL}/api/heatmap/${region}/${date}/${layer}`;
            const data = await this._fetch(url);
            this._cacheSet(cacheKey, data);
            console.log(`✅ Heatmap loaded: ${layer} on ${date} → ${data.maskUrl}`);
            return data;
        } catch (error) {
            console.warn(`⚠️ No heatmap for ${layer} on ${date}: ${error.message}`);
            return null;
        }
    }

    /**
     * Load Timeline (tổng hợp ngày từ cả 2 regions).
     * @returns { dates, dateRange, totalDays, regions }
     */
    async loadTimeline() {
        const cacheKey = 'timeline_all';
        const cached = this._cacheGet(cacheKey);
        if (cached) return cached;

        try {
            const data = await this._fetch(`${window.API_BASE_URL}/api/timeline`);
            this._cacheSet(cacheKey, data);
            console.log(`✅ Timeline loaded: ${data.totalDays} days`);
            return data;
        } catch (error) {
            console.error('❌ Error loading timeline:', error.message);
            return null;
        }
    }

    /**
     * Kiểm tra các layer có sẵn cho ngày cụ thể.
     * @returns { region, date, layers: {rain, soilMoisture, tide, flood, static}, hasAnyData }
     */
    async loadAvailableLayers(region, date) {
        const cacheKey = `layers_${region}_${date}`;
        const cached = this._cacheGet(cacheKey);
        if (cached) return cached;

        try {
            const url = `${window.API_BASE_URL}/api/available-layers/${region}/${date}`;
            const data = await this._fetch(url);
            this._cacheSet(cacheKey, data);
            return data;
        } catch (error) {
            console.warn(`⚠️ Could not check layers for ${region}/${date}: ${error.message}`);
            return null;
        }
    }

    /**
     * Load pixel history for a coordinate over a date range (bulk).
     * Uses /api/pixel/history endpoint instead of individual pixel calls.
     * @returns Array of { date, rainfall, soilMoisture, dem, slope, flow, landCover }
     */
    async loadPixelHistory(lat, lng, region, startDate, endDate) {
        const cacheKey = `pixhist_${region}_${lat}_${lng}_${startDate}_${endDate}`;
        const cached = this._cacheGet(cacheKey);
        if (cached) return cached;

        try {
            const url = `${window.API_BASE_URL}/api/pixel/history?lat=${lat}&lng=${lng}&region=${region}&startDate=${startDate}&endDate=${endDate}`;
            const data = await this._fetch(url);
            this._cacheSet(cacheKey, data);
            console.log(`✅ Pixel history [${lat}, ${lng}] ${startDate}→${endDate}: ${data.length} days`);
            return data;
        } catch (error) {
            console.warn(`⚠️ No pixel history at [${lat},${lng}]: ${error.message}`);
            return null;
        }
    }

    /**
     * Load monthly aggregated rainfall for seasonality chart.
     * Uses /api/pixel/monthly endpoint — much faster than per-day pixel history.
     * @param {number[]} years - Array of years to fetch, e.g. [2020,2021,2022,2023,2024,2025]
     * @returns { "2020": [12 monthly totals], "2021": [...], ... }
     */
    async loadMonthlyRainfall(lat, lng, region, years) {
        const yearsStr = years.join(',');
        const cacheKey = `monthly_${region}_${lat}_${lng}_${yearsStr}`;
        const cached = this._cacheGet(cacheKey);
        if (cached) return cached;

        try {
            const url = `${window.API_BASE_URL}/api/pixel/monthly?lat=${lat}&lng=${lng}&region=${region}&years=${yearsStr}`;
            const data = await this._fetch(url);
            this._cacheSet(cacheKey, data);
            console.log(`✅ Monthly rainfall [${lat}, ${lng}] years=${yearsStr}: loaded`);
            return data;
        } catch (error) {
            console.warn(`⚠️ Monthly rainfall failed: ${error.message}`);
            return null;
        }
    }

    /**
     * Load region-level aggregated history (forecast history).
     * Uses /api/forecast/:region/history endpoint.
     * @returns Array of { date, totalRainfall, avgSoilMoisture, avgDem, avgSlope, avgFlow, avgLandCover }
     */
    async loadRegionHistory(region, startDate, endDate) {
        const cacheKey = `reghist_${region}_${startDate}_${endDate}`;
        const cached = this._cacheGet(cacheKey);
        if (cached) return cached;

        try {
            const url = `${window.API_BASE_URL}/api/forecast/${region}/history?startDate=${startDate}&endDate=${endDate}`;
            const data = await this._fetch(url);
            this._cacheSet(cacheKey, data);
            console.log(`✅ Region history ${region} ${startDate}→${endDate}: ${data.length} days`);
            return data;
        } catch (error) {
            console.warn(`⚠️ No region history for ${region}: ${error.message}`);
            return null;
        }
    }

    /**
     * Prefetch heatmap cho ngày kế tiếp (background optimization)
     */
    async prefetchNextDate(region, currentDate) {
        const next = this._offsetDate(currentDate, 1);
        if (!next) return;
        console.log(`🔮 Prefetching ${next}...`);
        await this.loadHeatmapData(region, next, 'flood').catch(() => { });
    }

    /**
     * Get ML flood prediction for a pixel.
     * Uses /api/inference/pixel/:lat/:lng/:date/:region endpoint.
     * Returns pixel data enriched with mlPrediction.
     * @returns { ...pixelData, mlPrediction: { flood_risk, probability, confidence, model_version } }
     */
    async loadMlPrediction(lat, lng, date, region) {
        const cacheKey = `mlpred_${region}_${date}_${lat}_${lng}`;
        const cached = this._cacheGet(cacheKey);
        if (cached) return cached;

        try {
            const url = `${window.API_BASE_URL}/api/inference/pixel/${lat}/${lng}/${date}/${region}`;
            const data = await this._fetch(url);
            this._cacheSet(cacheKey, data);
            console.log(`🤖 ML Prediction [${lat}, ${lng}] on ${date}: ${data.mlPrediction?.flood_risk || 'N/A'} (${((data.mlPrediction?.confidence || 0) * 100).toFixed(0)}%)`);
            return data;
        } catch (error) {
            console.warn(`⚠️ ML prediction unavailable: ${error.message}`);
            return null;
        }
    }

    /**
     * Call inference API directly with feature values.
     * Uses POST /api/inference/predict endpoint.
     * @returns { flood_risk, probability, confidence, model_version, features_used }
     */
    async predictFloodRisk(features) {
        try {
            const url = `${window.API_BASE_URL}/api/inference/predict`;
            const response = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(features),
            });
            const envelope = await response.json();
            if (!envelope.success) throw new Error(envelope.error?.message || 'Prediction failed');
            return envelope.data;
        } catch (error) {
            console.warn(`⚠️ Direct prediction failed: ${error.message}`);
            return null;
        }
    }

    /**
     * Check ML inference service health.
     * @returns { status, model_loaded, model_version, uptime_seconds }
     */
    async checkInferenceHealth() {
        try {
            const data = await this._fetch(`${window.API_BASE_URL}/api/inference/health`);
            return data;
        } catch {
            return { status: 'unavailable', model_loaded: false, model_version: 'N/A', uptime_seconds: 0 };
        }
    }

    // ----------------------------------------------------------
    // Utilities
    // ----------------------------------------------------------

    _offsetDate(dateStr, offsetDays) {
        try {
            const d = new Date(dateStr + 'T00:00:00Z');
            d.setUTCDate(d.getUTCDate() + offsetDays);
            return d.toISOString().split('T')[0];
        } catch { return null; }
    }

    getNextDate(dateStr) { return this._offsetDate(dateStr, 1); }
    getPreviousDate(dateStr) { return this._offsetDate(dateStr, -1); }

    clearCache() {
        this.cache.clear();
        console.log('🗑️ DataLoader cache cleared');
    }

    getCacheStats() {
        return { size: this.cache.size, maxSize: this.maxCacheSize, keys: [...this.cache.keys()] };
    }
}

// -------------------------------------------------------
// Global singleton
// -------------------------------------------------------
const dataLoader = new DataLoader();

if (typeof window !== 'undefined') {
    window.dataLoader = dataLoader;
}

document.addEventListener('DOMContentLoaded', async () => {
    console.log('📊 Initializing DataLoader...');
    await dataLoader.loadAvailableDates('DaNang');
    console.log('✅ DataLoader ready');
});
