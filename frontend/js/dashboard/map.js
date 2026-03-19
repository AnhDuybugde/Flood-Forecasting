// Vietnam Flood Dashboard - Leaflet with OpenStreetMap Tiles
// OpenStreetMap tiles (giống Google Maps) + transparent heatmap overlay

let map;
let heatmapLayer;
let boundingBoxes = [];
let rainHeatLayer = null;   // L.heatLayer cho rainfall (tuỳ chọn)
let isUpdating = false;     // Lock để ngăn race condition
let _dataBoundsRect = null; // single rectangle for data extent visualization

let currentDate = null; // Will be auto-set from API (latest available date)
let currentRegion = 'DaNang';
let vietnamBoundary = null; // Polygon biên giới VN để mask vùng ngập

// ============================================================
// REGION BOUNDS - khai báo sớm để getRegion() có thể dùng ngay
// ============================================================
const REGION_BOUNDS = {
    DaNang: {
        north: 16.25, south: 15.95,
        east: 108.40, west: 107.90,
        rows: 20, cols: 20
    }
};

/** Leaflet [[south, west], [north, east]] from REGION_BOUNDS entry */
function leafletBoundsFromRegion(rb) {
    if (!rb) return null;
    return [[rb.south, rb.west], [rb.north, rb.east]];
}

// ============================================================
// LAYER MANAGER (Config & UI Checkbox Toggles)
// ============================================================
const LayerManager = window.LayerManager = {
    // Configuration: Mapping UI Checkbox IDs to API Layer Names
    config: {
        'cb-flood': { layer: 'label', name: 'Flood Risk', type: 'grid', color: '#FF1744' },
        'cb-rain': { layer: 'rain', name: 'Rainfall', type: 'heatmap', color: '#2196F3' },
        'cb-moisture': { layer: 'soilMoisture', name: 'Soil Moisture', type: 'heatmap', color: '#795548' },
        'cb-dem': { layer: 'static', name: 'DEM', type: 'image', color: '#9E9E9E' }
    },

    availability: {},
    currentDate: null,
    currentRegion: null,

    init: function () {
        console.log('🛡️ Initializing Layer Manager...');
        for (const [id, conf] of Object.entries(this.config)) {
            const checkbox = document.getElementById(id);
            if (checkbox) {
                checkbox.addEventListener('change', () => {
                    console.log(`Layer toggle: ${conf.layer} = ${checkbox.checked}`);
                    if (typeof updateHeatmap === 'function' && this.currentDate && this.currentRegion) {
                        updateHeatmap(this.currentDate, this.currentRegion, true);
                    }
                });
            }
        }
    },

    updateAvailability: async function (date, region) {
        this.currentDate = date;
        this.currentRegion = region;

        // Tất cả các layers tiêu chuẩn luôn có sẵn thông qua bộ prebuilt lưới hoặc static map
        this.availability = {
            label: true,
            rain: true,
            soilMoisture: true,
            static: true
        };
        this.updateUI();
    },

    updateUI: function () {
        for (const [id, conf] of Object.entries(this.config)) {
            const checkbox = document.getElementById(id);
            if (!checkbox) continue;
            let isAvailable = (conf.layer === 'static') ? (this.availability['static'] === true) : (this.availability[conf.layer] === true);
            const labelSpan = checkbox.parentElement.querySelector('span');

            if (isAvailable) {
                checkbox.disabled = false;
                checkbox.parentElement.classList.remove('opacity-50', 'cursor-not-allowed');
                if (labelSpan) labelSpan.textContent = conf.name;
            } else {
                checkbox.disabled = true;
                checkbox.parentElement.classList.add('opacity-50', 'cursor-not-allowed');
                if (labelSpan) labelSpan.textContent = `${conf.name} (nodata)`;
            }
        }
    },

    getActiveLayers: function () {
        const active = [];
        for (const [id, conf] of Object.entries(this.config)) {
            const checkbox = document.getElementById(id);
            if (checkbox && checkbox.checked && !checkbox.disabled) active.push(conf);
        }
        return active;
    }
};

// Initializer Hook for LayerManager
document.addEventListener('DOMContentLoaded', () => { setTimeout(() => { LayerManager.init(); }, 800); });


// ============================================================
// PNG MASK OVERLAY — 1 ImageOverlay replaces 400 DOM rectangles
// ============================================================
let _maskOverlay = null;
let _maskAbortController = null;

/**
 * Render flood mask as a single PNG ImageOverlay.
 * Pipeline (build_mask_png.py) generates 20x20 RGBA PNGs:
 *   - Red (255,50,50,140) = flood
 *   - Grey (128,128,128,100) = cloud/nodata
 *   - Transparent = no flood
 *
 * @param {string} region - Region name (e.g. 'DaNang')
 * @param {string} date   - Date string YYYY-MM-DD
 * @returns {Promise<boolean>} - Whether mask was loaded
 */
async function renderFloodMask(region, date) {
    // Cancel previous
    if (_maskAbortController) _maskAbortController.abort();
    _maskAbortController = new AbortController();

    const bounds = leafletBoundsFromRegion(REGION_BOUNDS[region]);
    if (!bounds) {
        console.warn('⚠️ No mask bounds for region:', region);
        return false;
    }

    const maskUrl = `${window.API_BASE_URL || ''}/api/mask/${region}/${date}/label.png`;

    try {
        const res = await fetchWithTimeout(maskUrl, {
            signal: _maskAbortController.signal,
        }, 5000);

        if (!res.ok) {
            console.log('ℹ️ Mask PNG not available, using legacy grid');
            return false;
        }

        const blob = await res.blob();
        const objectUrl = URL.createObjectURL(blob);

        if (_maskOverlay) {
            // Update existing overlay — no DOM churn
            _maskOverlay.setUrl(objectUrl);
            _maskOverlay.setBounds(bounds);
        } else {
            // Create new overlay — 1 DOM element total
            _maskOverlay = L.imageOverlay(objectUrl, bounds, {
                opacity: 0.6,
                interactive: false,
                className: 'flood-mask-overlay',
                zIndex: 400,
            }).addTo(map);
        }

        console.log('✅ Flood mask loaded (1 DOM node vs 400 rects)');
        return true;
    } catch (err) {
        if (err.name === 'AbortError') return false;
        console.warn('⚠️ Mask load failed:', err.message);
        return false;
    }
}

/** Remove flood mask overlay from map */
function clearFloodMask() {
    if (_maskOverlay) {
        map.removeLayer(_maskOverlay);
        _maskOverlay = null;
    }
}

/** Update (or create) a single bounds rectangle for visual debugging */
function setDataBoundsRect(bounds) {
    if (!map || !bounds) return;
    const leafletBounds = [[bounds.south, bounds.west], [bounds.north, bounds.east]];

    if (_dataBoundsRect) {
        _dataBoundsRect.setBounds(leafletBounds);
        return;
    }
    _dataBoundsRect = L.rectangle(leafletBounds, {
        color: '#2196F3',
        weight: 1,
        fill: false,
        dashArray: '5, 5',
        interactive: false,
    }).addTo(map);
}

function clearDataBoundsRect() {
    if (_dataBoundsRect && map) {
        map.removeLayer(_dataBoundsRect);
        _dataBoundsRect = null;
    }
}

// ... existing code ...

// Pixel Parameters Panel logic removed to prefer only Map Popup

// ============================================================
// PERFORMANCE: AbortController + Debounce + Throttle + Dedup for map clicks
// ============================================================
let _pixelAbortController = null;
let _lastClickTime = 0;
let _clickDebounceTimer = null;
const CLICK_THROTTLE_MS = 300;
const CLICK_DEBOUNCE_MS = 150;

// Inference-ready: Cache last N pixel results to avoid duplicate API calls
const _pixelResultCache = new Map();
const PIXEL_RESULT_CACHE_MAX = 50;

// Request Deduplication: Reuse in-flight fetch promises (same URL = same promise)
const _inflightRequests = new Map();

// Track which date was requested to detect stale responses
let _pendingPixelDate = null;

function getPixelCacheKey(lat, lng, date) {
    return `${lat.toFixed(4)}_${lng.toFixed(4)}_${date}`;
}

// Client-side grid data cache (populated when /api/grid is loaded)
let _cachedGridData = {}; // { 'label_DaNang_2026-01-31': { data, bounds, width, height } }

/**
 * Try to read pixel value from already-loaded grid data (0ms, no API call).
 * Returns value or null if grid not cached.
 */
function readPixelFromCachedGrid(layer, region, date, lat, lng) {
    const key = `${layer}_${region}_${date}`;
    const grid = _cachedGridData[key];
    if (!grid || !grid.data) return undefined; // undefined = not cached

    const { bounds, width, height, data } = grid;
    const west = bounds.west, east = bounds.east, north = bounds.north, south = bounds.south;
    const col = Math.floor((lng - west) / (east - west) * width);
    const row = Math.floor((north - lat) / (north - south) * height);

    if (col < 0 || col >= width || row < 0 || row >= height) return null;
    return data[row * width + col];
}

// ============================================================
// NORMALIZE DATE FOR PIXEL URL (always one path segment: YYYY-MM-DD)
// Prevents broken route when date is DD/MM/YYYY (e.g. 03/01/2020 -> date=03, region=01)
// ============================================================
function normalizeDateForUrl(value) {
    if (value == null || value === undefined) return null;
    const s = String(value).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    const m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (m) {
        const d = parseInt(m[1], 10), mo = parseInt(m[2], 10), y = parseInt(m[3], 10);
        if (mo >= 1 && mo <= 12 && d >= 1 && d <= 31)
            return y + '-' + String(mo).padStart(2, '0') + '-' + String(d).padStart(2, '0');
    }
    return null;
}

// ============================================================
// FETCH WITH TIMEOUT + DEDUPLICATION
// ============================================================
// NOTE: Pixel API can be slow when backend is forced to full-download GeoTIFF/NPZ (no HTTP Range).
// Keep this high enough to avoid false timeouts on constrained networks.
const PIXEL_FETCH_TIMEOUT_MS = 30000; // 30 seconds max

/**
 * Deduplicated fetch: If same URL is already in-flight, reuse that promise.
 * Prevents duplicate API calls when user clicks rapidly at same location.
 */
function fetchWithTimeout(url, options = {}, timeoutMs = PIXEL_FETCH_TIMEOUT_MS) {
    // Check for in-flight request with same URL (deduplication)
    const dedupKey = url;
    if (!options.signal && _inflightRequests.has(dedupKey)) {
        console.log('🔄 Reusing in-flight request:', dedupKey.split('/').slice(-4).join('/'));
        return _inflightRequests.get(dedupKey);
    }

    const promise = new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            reject(new Error(`Request timed out after ${timeoutMs}ms`));
        }, timeoutMs);

        fetch(url, options)
            .then(res => { clearTimeout(timer); resolve(res); })
            .catch(err => { clearTimeout(timer); reject(err); });
    });

    // Store and auto-cleanup
    if (!options.signal) {
        _inflightRequests.set(dedupKey, promise);
        promise.finally(() => _inflightRequests.delete(dedupKey));
    }

    return promise;
}

// ============================================================
// FETCH WITH RETRY (for large grid downloads)
// ============================================================
async function fetchWithRetryTimeout(url, {
    timeoutMs = 60000,
    retries = 2,
    backoffMs = 800,
    signal,
} = {}) {
    let lastErr = null;
    for (let attempt = 0; attempt <= retries; attempt++) {
        const controller = new AbortController();
        const t = setTimeout(() => controller.abort(), timeoutMs);

        // If caller provided a signal, abort this request too
        const onAbort = () => controller.abort();
        if (signal) signal.addEventListener('abort', onAbort, { once: true });

        try {
            const res = await fetch(url, { signal: controller.signal });
            clearTimeout(t);
            if (signal) signal.removeEventListener('abort', onAbort);

            // Retry on transient failures (queue full / gateway timeout)
            if ((res.status === 503 || res.status === 504) && attempt < retries) {
                const sleep = backoffMs * Math.pow(2, attempt);
                await new Promise(r => setTimeout(r, sleep));
                continue;
            }
            return res;
        } catch (e) {
            clearTimeout(t);
            if (signal) signal.removeEventListener('abort', onAbort);
            lastErr = e;
            if (attempt < retries) {
                const sleep = backoffMs * Math.pow(2, attempt);
                await new Promise(r => setTimeout(r, sleep));
                continue;
            }
        }
    }
    throw lastErr || new Error('Grid fetch failed');
}

// ============================================================
// SAFE POPUP CONTENT BUILDER - Prevents template literal crashes
// ============================================================
function buildSelectedAreaPopupContent(data, lat, lng, region, date) {
    try {
        const risk = data.floodRisk || 'LOW';
        const riskColor = getRiskColor(risk);
        const regionName = region === 'DaNang' ? 'Đà Nẵng' : (region === 'DBSCL' ? 'Đồng Bằng Sông Cửu Long' : region);

        // Safe value formatters (backend now returns de-normalized physical values)
        const fmtVal = (v, unit, decimals = 2) => {
            if (v == null || v === undefined) return 'nodata';
            const num = Number(v);
            if (isNaN(num)) return 'nodata';
            return num.toFixed(decimals) + unit;
        };
        const fmtPct = (v) => {
            if (v == null || v === undefined) return 'nodata';
            const num = Number(v);
            if (isNaN(num)) return 'nodata';
            return (num * 100).toFixed(1) + '%';
        };
        const fmtLandCover = (v) => {
            if (v == null || v === undefined) return 'nodata';
            const num = Number(v);
            if (isNaN(num)) return 'nodata';
            if (num < 0.05) return 'nodata';
            if (num < 0.15) return 'Trees';
            if (num < 0.25) return 'Shrubland';
            if (num < 0.35) return 'Grassland';
            if (num < 0.45) return 'Cropland';
            if (num < 0.55) return 'Built-up';
            if (num < 0.65) return 'Bare / Sparse';
            if (num < 0.75) return 'Snow / Ice';
            if (num < 0.85) return 'Water body';
            if (num < 0.95) return 'Wetland';
            return 'Mangroves';
        };
        const fmtFlood = (v) => {
            if (v == null || v === undefined) return 'nodata';
            const num = Number(v);
            if (isNaN(num)) return 'nodata';
            return (num * 100).toFixed(0) + '%';
        };

        return `
            <div class="flood-popup-card" style="font-family: Inter, sans-serif; min-width: 224px; max-width: 272px; color: #4b5563;">
                <!-- Header -->
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; border-bottom: 2px solid #f3f4f6; padding-bottom: 10px;">
                    <strong style="font-size: 12px; color: #1e293b; font-weight: 700;">
                        <span style="margin-right: 5px;">📍</span>Selected Area
                    </strong>
                    <span style="background: ${riskColor}; color: white; font-size: 9px; font-weight: 700; padding: 3px 10px; border-radius: 5px; text-transform: uppercase; letter-spacing: 0.5px; box-shadow: 0 2px 4px ${riskColor}40;">
                        ${risk === 'HIGH' || risk === 'CRITICAL' ? '⚠ ' : ''}${risk}
                    </span>
                </div>

                <!-- Meta -->
                <div style="font-size: 10px; margin-bottom: 10px;">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
                        <span style="color: #64748b;">Coordinates:</span>
                        <span style="font-family: 'JetBrains Mono', monospace; font-weight: 500; font-size: 10px; color: #334155;">${lat.toFixed(4)}°N, ${lng.toFixed(4)}°E</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; padding-bottom: 10px; border-bottom: 1px solid #f3f4f6;">
                        <span style="color: #64748b;">Region:</span>
                        <span style="font-weight: 600; color: #475569;">${regionName}</span>
                    </div>
                </div>

                <!-- Environmental Data -->
                <div style="font-size: 10px; line-height: 1.8;">
                    <strong style="font-size: 10px; color: #1e293b; display: block; margin-bottom: 6px;">
                        <span style="margin-right: 4px;">🌊</span>Environmental Data
                    </strong>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 3px;">
                        <span style="color: #64748b;">🌧 Rainfall (24h):</span>
                        <span style="font-weight: 600; color: #1d4ed8;">${fmtVal(data.rainfall, ' mm', 1)}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 3px;">
                        <span style="color: #64748b;">Soil Moisture:</span>
                        <span style="font-weight: 600; color: #dc2626;">${fmtPct(data.soilMoisture)}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 3px;">
                        <span style="color: #64748b;">Tide Level:</span>
                        <span style="font-weight: 600; color: #0891b2;">${fmtVal(data.tide, ' m', 2)}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 3px;">
                        <span style="color: #64748b;">Flood Probability:</span>
                        <span style="font-weight: 600; color: #b91c1c;">${fmtFlood(data.flood)}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 3px;">
                        <span style="color: #64748b;">DEM Index:</span>
                        <span style="font-weight: 600; color: #059669;">${fmtVal(data.dem, '', 4)}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 3px;">
                        <span style="color: #64748b;">Slope:</span>
                        <span style="font-weight: 600; color: #7e22ce;">${fmtVal(data.slope, '°', 1)}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 3px;">
                        <span style="color: #64748b;">Flow Accumulation:</span>
                        <span style="font-weight: 600; color: #0e7490;">${fmtVal(data.flow, '', 0)}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 3px;">
                        <span style="color: #64748b;">Land Cover:</span>
                        <span style="font-weight: 500; color: #475569;">${fmtLandCover(data.landCover)}</span>
                    </div>

                    <!-- Footer -->
                    <div style="display: flex; justify-content: space-between; margin-bottom: 12px; padding-top: 6px; border-top: 1px solid #f3f4f6;">
                        <span style="color: #64748b;">📅 Date:</span>
                        <span style="font-weight: 500; color: #334155;">${date}</span>
                    </div>
                    
                    <button onclick="window.location.href='/detail.html?lat=${lat}&lng=${lng}&date=${date}&region=${region}'" 
                        style="width: 100%; background: linear-gradient(135deg, #1976d2, #1565c0); color: white; border: none; padding: 10px; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 10px; display: flex; align-items: center; justify-content: center; gap: 6px; transition: all 0.2s; box-shadow: 0 2px 8px rgba(25,118,210,0.3);"
                        onmouseover="this.style.transform='translateY(-1px)'; this.style.boxShadow='0 4px 12px rgba(25,118,210,0.4)';" 
                        onmouseout="this.style.transform='translateY(0)'; this.style.boxShadow='0 2px 8px rgba(25,118,210,0.3)';">
                        <span class="material-icons" style="font-size: 16px;">assessment</span>
                        View Detailed Dashboard
                    </button>
                </div>
            </div>
        `;
    } catch (templateError) {
        console.error('❌ Error building popup content:', templateError);
        return `
            <div style="font-family: Inter, sans-serif; padding: 12px; text-align: center; color: #64748b;">
                <p style="font-weight: 600; margin: 0 0 4px;">Selected Area</p>
                <p style="font-size: 12px; margin: 0;">${lat.toFixed(4)}°N, ${lng.toFixed(4)}°E</p>
                <p style="font-size: 11px; color: #94a3b8; margin: 4px 0 0;">Data display error. <a href="/detail.html?lat=${lat}&lng=${lng}&date=${date}&region=${region}" style="color: #1976d2;">View Details</a></p>
            </div>
        `;
    }
}

// ============================================================
// LOADING POPUP CONTENT - Shows immediately on click
// ============================================================
function buildLoadingPopupContent(lat, lng) {
    return `
        <div class="flood-popup-card" style="font-family: Inter, sans-serif; min-width: 260px; max-width: 320px; color: #4b5563; padding: 8px 0;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                <strong style="font-size: 15px; color: #1e293b; font-weight: 700;">
                    <span style="margin-right: 6px;">📍</span>Selected Area
                </strong>
                <span style="background: #e2e8f0; color: #64748b; font-size: 11px; font-weight: 600; padding: 4px 10px; border-radius: 6px;">LOADING</span>
            </div>
            <div style="font-size: 12px; color: #94a3b8; margin-bottom: 12px;">
                ${lat.toFixed(4)}°N, ${lng.toFixed(4)}°E
            </div>
            <!-- Skeleton Loading -->
            <div style="space-y: 8px;">
                <div style="height: 14px; background: linear-gradient(90deg, #f1f5f9 25%, #e2e8f0 50%, #f1f5f9 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; border-radius: 4px; margin-bottom: 8px;"></div>
                <div style="height: 14px; background: linear-gradient(90deg, #f1f5f9 25%, #e2e8f0 50%, #f1f5f9 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; border-radius: 4px; margin-bottom: 8px; width: 80%;"></div>
                <div style="height: 14px; background: linear-gradient(90deg, #f1f5f9 25%, #e2e8f0 50%, #f1f5f9 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; border-radius: 4px; margin-bottom: 8px; width: 60%;"></div>
                <div style="height: 14px; background: linear-gradient(90deg, #f1f5f9 25%, #e2e8f0 50%, #f1f5f9 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; border-radius: 4px; width: 90%;"></div>
            </div>
            <div style="text-align: center; margin-top: 16px; font-size: 12px; color: #94a3b8;">
                <span style="display: inline-block; animation: spin 1s linear infinite; font-size: 16px;">⏳</span>
                Fetching environmental data...
            </div>
        </div>
    `;
}

// Handle map click (with debounce + throttle + abort + loading state + timeout)
async function handleMapClick(e) {
    console.log('🖱️ === MAP CLICK EVENT FIRED ===');
    console.log('🖱️ Click coords:', e.latlng.lat.toFixed(4), e.latlng.lng.toFixed(4));
    console.log('🖱️ currentDate:', currentDate, '| currentRegion:', currentRegion);
    console.log('🖱️ API_BASE_URL:', JSON.stringify(window.API_BASE_URL));

    // Debounce: cancel rapid successive clicks, only process the last one
    if (_clickDebounceTimer) {
        clearTimeout(_clickDebounceTimer);
        _clickDebounceTimer = null;
    }

    // Throttle: skip if called too quickly
    const now = Date.now();
    if (now - _lastClickTime < CLICK_THROTTLE_MS) {
        console.log('⏳ Click throttled, scheduling debounce...');
        _clickDebounceTimer = setTimeout(() => handleMapClick(e), CLICK_DEBOUNCE_MS);
        return;
    }
    _lastClickTime = now;

    // Abort previous in-flight request
    if (_pixelAbortController) {
        _pixelAbortController.abort();
    }
    _pixelAbortController = new AbortController();
    const requestDate = currentDate; // Capture date at request time
    _pendingPixelDate = requestDate;
    const { lat, lng } = e.latlng;

    // Check if click is in data region
    const region = getRegion(lat, lng);

    if (!region) {
        // Vẫn hiện popup thông báo nếu click ngoài vùng dữ liệu
        console.log('⚠️ Click outside data regions');
        L.popup({ className: 'flood-popup', maxWidth: 340, autoPan: true, autoPanPadding: [40, 40] })
            .setLatLng(e.latlng)
            .setContent(`
                <div style="font-family: Inter, sans-serif; padding: 8px; text-align: center; color: #64748b;">
                    <span style="font-size: 32px; display: block; margin-bottom: 8px;">🚫</span>
                    <p style="margin: 4px 0 0; font-weight: 700; font-size: 14px; color: #334155;">No Data Available</p>
                    <p style="margin: 6px 0 0; font-size: 12px; line-height: 1.5;">This location is outside the data coverage area.<br/>Currently supported: <strong>Đà Nẵng</strong></p>
                    <p style="margin: 8px 0 0; font-size: 11px; color: #94a3b8;">${lat.toFixed(4)}°N, ${lng.toFixed(4)}°E</p>
                </div>
            `)
            .openOn(map);
        return;
    }

    console.log(`📍 Clicked in ${region} at`, lat.toFixed(4), lng.toFixed(4));

    // Auto-switch region if different
    if (region !== currentRegion) {
        console.log(`🔄 Switching region from ${currentRegion} to ${region}`);
        currentRegion = region;
        if (typeof updateHeatmap === 'function') {
            updateHeatmap(currentDate, currentRegion, true);
        }
    }

    // ====== STEP 1: Show loading popup IMMEDIATELY ======
    const loadingPopup = L.popup({ className: 'flood-popup', maxWidth: 360, minWidth: 280, autoPan: true, autoPanPadding: [40, 40] })
        .setLatLng(e.latlng)
        .setContent(buildLoadingPopupContent(lat, lng))
        .openOn(map);

    console.log('💬 Loading popup shown');

    try {
        // ====== STEP 2: Fetch pixel data WITH TIMEOUT ======
        // Use YYYY-MM-DD in URL so path has one segment (DD/MM/YYYY would break route)
        const dateForUrl = normalizeDateForUrl(currentDate) || currentDate;
        const url = `${window.API_BASE_URL || ''}/api/pixel/${lat}/${lng}/${dateForUrl}/${region}`;
        console.log('🔗 Fetching URL:', url);
        const t0 = performance.now();

        const response = await fetchWithTimeout(url, { signal: _pixelAbortController.signal }, PIXEL_FETCH_TIMEOUT_MS);
        console.log('📨 Response status:', response.status);

        let envelope;
        try {
            envelope = await response.json();
        } catch (jsonError) {
            console.error('❌ Failed to parse API response as JSON:', jsonError);
            envelope = { success: false, error: { message: `Server returned invalid response (HTTP ${response.status})` } };
        }

        let data;
        const elapsed = (performance.now() - t0).toFixed(0);

        if (!envelope.success) {
            console.warn(`⚠️ API Error: ${envelope.error?.message || response.status}`);
            data = { floodRisk: 'nodata', rainfall: null, dem: null, slope: null, soilMoisture: null, flow: null, landCover: null, tide: null };
        } else if (envelope.data == null || envelope.data === undefined) {
            console.warn('⚠️ API returned success but no data');
            data = { floodRisk: 'nodata', rainfall: null, dem: null, slope: null, soilMoisture: null, flow: null, landCover: null, tide: null };
        } else {
            data = envelope.data;
            console.log(`✅ Pixel data fetched in ${elapsed}ms (server: ${data.metadata?.responseTimeMs || '?'}ms)`);
        }

        // Stale response detection: if date changed while request was in-flight, discard
        if (_pendingPixelDate !== requestDate) {
            console.log('🔄 Discarding stale pixel response (date changed)');
            return;
        }

        // ====== STEP 3: Update popup with real data (smooth transition) ======
        const popupContent = buildSelectedAreaPopupContent(data, lat, lng, region, currentDate);

        // Always try to update or re-open the popup
        try {
            if (loadingPopup && loadingPopup.isOpen && loadingPopup.isOpen()) {
                loadingPopup.setContent(popupContent);
                loadingPopup.update();
                console.log('✅ Popup updated with pixel data');
            } else {
                // Re-open popup if user closed loading state or popup was lost
                L.popup({ className: 'flood-popup', maxWidth: 360, minWidth: 280, autoPan: true, autoPanPadding: [40, 40] })
                    .setLatLng(e.latlng)
                    .setContent(popupContent)
                    .openOn(map);
                console.log('✅ Popup re-opened with pixel data');
            }
        } catch (popupErr) {
            console.warn('⚠️ Failed to update popup, opening new one:', popupErr);
            L.popup({ className: 'flood-popup', maxWidth: 360, minWidth: 280, autoPan: true, autoPanPadding: [40, 40] })
                .setLatLng(e.latlng)
                .setContent(popupContent)
                .openOn(map);
        }

    } catch (error) {
        if (error.name === 'AbortError') {
            console.log('🔄 Previous click aborted (newer click in progress)');
            return;
        }
        console.error('❌ Error fetching pixel data:', error);

        const isTimeout = error.message && error.message.includes('timed out');
        const errorContent = `
            <div style="font-family: Inter, sans-serif; padding: 12px; text-align: center; color: #dc2626; min-width: 240px;">
                <span style="font-size: 32px; display: block; margin-bottom: 8px;">${isTimeout ? '⏱️' : '⚠️'}</span>
                <p style="margin: 4px 0 0; font-weight: 700; font-size: 14px; color: #334155;">${isTimeout ? 'Request Timed Out' : 'Error Loading Data'}</p>
                <p style="margin: 6px 0 0; font-size: 12px; color: #64748b; line-height: 1.5;">${isTimeout ? 'The server took too long to respond.<br/>Please try again.' : (error.message || 'Network error')}</p>
                <p style="margin: 8px 0 0; font-size: 11px; color: #94a3b8;">${lat.toFixed(4)}°N, ${lng.toFixed(4)}°E</p>
                <button onclick="handleMapClick({latlng: {lat: ${lat}, lng: ${lng}}})" 
                    style="margin-top: 12px; padding: 8px 20px; background: #1976d2; color: white; border: none; border-radius: 6px; font-weight: 600; font-size: 12px; cursor: pointer; transition: background 0.2s;"
                    onmouseover="this.style.background='#1565c0'" onmouseout="this.style.background='#1976d2'">
                    🔄 Retry
                </button>
            </div>
        `;

        // Try to update existing popup, otherwise create new
        try {
            if (loadingPopup && loadingPopup.isOpen && loadingPopup.isOpen()) {
                loadingPopup.setContent(errorContent);
            } else {
                L.popup({ className: 'flood-popup', maxWidth: 360, autoPan: true })
                    .setLatLng(e.latlng)
                    .setContent(errorContent)
                    .openOn(map);
            }
        } catch (popupErr) {
            L.popup({ className: 'flood-popup', maxWidth: 360, autoPan: true })
                .setLatLng(e.latlng)
                .setContent(errorContent)
                .openOn(map);
        }
    }
}

// Get region from coordinates
function getRegion(lat, lng) {
    const db = REGION_BOUNDS.DaNang;
    if (lat <= db.north && lat >= db.south && lng <= db.east && lng >= db.west) {
        return 'DaNang';
    }
    return null;
}

// Get risk color
function getRiskColor(risk) {
    const colors = {
        'LOW': '#4CAF50',
        'MEDIUM': '#FFC107',
        'HIGH': '#F44336',
        'CRITICAL': '#B71C1C'
    };
    return colors[risk] || '#9E9E9E';
}

// ============================================================
// FLOOD MASK LAYER - Hiển thị vùng ngập lụt dạng polygon đỏ
// Canvas overlay để lưới đỏ scale đúng khi zoom in/out
// ============================================================

// Bounds của từng region - PHAI khop voi seed_sample_data.py va server/api.js

/**
 * Custom Leaflet layer: vẽ grid lên canvas, scale đúng khi zoom in/out.
 * Redraw mỗi khi map move/zoom để lưới luôn khớp với bản đồ (tọa độ địa lý).
 */
function createGridCanvasOverlay(gridData) {
    const width = gridData.size?.c ?? gridData.width;
    const height = gridData.size?.r ?? gridData.height;
    const bounds = {
        north: gridData.bounds.n ?? gridData.bounds.north,
        south: gridData.bounds.s ?? gridData.bounds.south,
        east:  gridData.bounds.e ?? gridData.bounds.east,
        west:  gridData.bounds.w ?? gridData.bounds.west,
    };
    const data = gridData.data;
    const latStep = (bounds.north - bounds.south) / height;
    const lngStep = (bounds.east - bounds.west) / width;
    const nodata = gridData.nodata ?? -9999;

    const GridCanvasLayer = L.Layer.extend({
        initialize: function (opts) {
            this._grid = { width, height, bounds, data, latStep, lngStep, nodata, scale: gridData.scale || 1 };
            L.setOptions(this, opts || {});
            this._raf = null;
        },
        onAdd: function (map) {
            this._map = map;
            const canvas = (this._canvas = document.createElement('canvas'));
            canvas.style.cssText = 'pointer-events:none;position:absolute;left:0;top:0;';
            const pane = map.getPane ? map.getPane('overlayPane') : map.getPanes().overlayPane;
            if (pane) pane.appendChild(canvas);
            this._draw();
            // Use continuous events to avoid visible "scale drift" during zoom animation.
            // RAF-throttle keeps it affordable.
            map.on('move zoom zoomanim resize moveend zoomend', this._draw, this);
        },
        onRemove: function (map) {
            map.off('move zoom zoomanim resize moveend zoomend', this._draw, this);
            if (this._raf) {
                cancelAnimationFrame(this._raf);
                this._raf = null;
            }
            const pane = map.getPane ? map.getPane('overlayPane') : map.getPanes().overlayPane;
            if (this._canvas && pane && pane.contains(this._canvas)) {
                pane.removeChild(this._canvas);
            }
            this._canvas = null;
        },
        _draw: function () {
            if (this._raf) return;
            this._raf = requestAnimationFrame(() => {
                this._raf = null;
                this._drawNow();
            });
        },
        _drawNow: function () {
            const map = this._map;
            if (!map || !this._canvas) return;

            const size = map.getSize();
            if (!size || size.x <= 0 || size.y <= 0) return;

            // HiDPI-safe canvas (prevents blur + helps alignment)
            const dpr = window.devicePixelRatio || 1;
            const targetW = Math.round(size.x * dpr);
            const targetH = Math.round(size.y * dpr);
            if (this._canvas.width !== targetW || this._canvas.height !== targetH) {
                this._canvas.width = targetW;
                this._canvas.height = targetH;
                this._canvas.style.width = size.x + 'px';
                this._canvas.style.height = size.y + 'px';
            }

            const ctx = this._canvas.getContext('2d');
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
            ctx.clearRect(0, 0, size.x, size.y);

            const g = this._grid;
            const zoom = map.getZoom();
            const step = zoom < 10 ? 8 : zoom < 12 ? 4 : zoom < 14 ? 2 : 1;
            const sw = map.getBounds().getSouthWest();
            const ne = map.getBounds().getNorthEast();

            const gScale = g.scale && g.scale > 0 ? g.scale : 1;
            for (let row = 0; row < g.height; row += step) {
                for (let col = 0; col < g.width; col += step) {
                    const raw = g.data[row * g.width + col];
                    if (raw == null || raw <= 0 || raw <= -9998 || raw === g.nodata) continue;
                    const valNorm = gScale > 1 && Number(raw) > 1 ? Math.min(1, Number(raw) / gScale) : Math.min(1, Number(raw));
                    if (!(valNorm > 0)) continue;

                    const cellNorth = g.bounds.north - row * g.latStep;
                    const cellSouth = g.bounds.north - (row + step) * g.latStep;
                    const cellWest = g.bounds.west + col * g.lngStep;
                    const cellEast = g.bounds.west + (col + step) * g.lngStep;

                    if (cellNorth < sw.lat || cellSouth > ne.lat || cellEast < sw.lng || cellWest > ne.lng) continue;

                    const pt1 = map.latLngToContainerPoint(L.latLng(cellSouth, cellWest));
                    const pt2 = map.latLngToContainerPoint(L.latLng(cellNorth, cellEast));

                    const x = Math.floor(Math.min(pt1.x, pt2.x));
                    const y = Math.floor(Math.min(pt1.y, pt2.y));
                    const w = Math.ceil(Math.abs(pt2.x - pt1.x));
                    const h = Math.ceil(Math.abs(pt2.y - pt1.y));

                    if (w < 1 || h < 1) continue;

                    const opacity = Math.max(0.15, Math.min(0.8, valNorm));
                    ctx.fillStyle = `rgba(239, 68, 68, ${opacity.toFixed(2)})`;
                    ctx.fillRect(x, y, w, h);
                }
            }
        },
    });

    return new GridCanvasLayer();
}

/**
 * Convert categorical grid (e.g. label) into a PNG dataURL and render via Leaflet ImageOverlay.
 * This avoids zoom/pan drift and provides smooth zoom animation.
 */
function createGridImageOverlay(gridData, boundsObj) {
    const width = gridData.size?.c ?? gridData.width;
    const height = gridData.size?.r ?? gridData.height;
    const data = gridData.data;
    const nodata = gridData.nodata ?? -9999;

    // Create tiny canvas (grid resolution). Browser + Leaflet will scale it smoothly.
    const canvas = document.createElement('canvas');
    canvas.width = width;
    canvas.height = height;
    const ctx = canvas.getContext('2d', { willReadFrequently: true });
    if (!ctx) {
        // Hard fallback if canvas context fails for any reason
        return createGridCanvasOverlay(gridData);
    }

    const img = ctx.createImageData(width, height);
    const px = img.data;
    const scale = gridData.scale && gridData.scale > 0 ? gridData.scale : 1;

    // 1 ảnh pixel = 1 ô lưới (khớp /api/grid bounds). Hàng 0 = bắc.
    for (let i = 0; i < width * height; i++) {
        const v = data[i];
        const o = i * 4;
        if (v == null || v === nodata || v <= -9998 || !(Number(v) > 0)) {
            px[o + 0] = px[o + 1] = px[o + 2] = px[o + 3] = 0;
            continue;
        }
        const vn = scale > 1 && Number(v) > 1 ? Math.min(1, Number(v) / scale) : Math.min(1, Number(v));
        if (!(vn > 0)) {
            px[o + 0] = px[o + 1] = px[o + 2] = px[o + 3] = 0;
            continue;
        }
        px[o + 0] = 239;
        px[o + 1] = 68;
        px[o + 2] = 68;
        px[o + 3] = Math.max(40, Math.min(255, Math.floor(vn * 200 + 55)));
    }

    ctx.putImageData(img, 0, 0);
    const dataUrl = canvas.toDataURL('image/png');

    const b = boundsObj || {
        north: gridData.bounds?.n ?? gridData.bounds?.north,
        south: gridData.bounds?.s ?? gridData.bounds?.south,
        east: gridData.bounds?.e ?? gridData.bounds?.east,
        west: gridData.bounds?.w ?? gridData.bounds?.west,
    };

    return L.imageOverlay(dataUrl, [[b.south, b.west], [b.north, b.east]], {
        opacity: 0.6,
        interactive: false,
        className: 'flood-mask-overlay',
        zIndex: 400,
    });
}

/**
 * Render Grid GeoTIFF layer on map (Canvas overlay - scale theo zoom)
 */
async function renderGridLayer(date, region, layerName) {
    try {
        const url = `${window.API_BASE_URL || ''}/api/grid/${region}/${date}/${layerName}?format=bin`;
        const response = await fetchWithRetryTimeout(url, {
            timeoutMs: 90000, // grid can be large on slow networks
            retries: 2,
            backoffMs: 800,
        });
        const contentType = response.headers.get('content-type') || '';

        let grid;
        if (contentType.includes('octet-stream')) {
            const buf = await response.arrayBuffer();
            const view = new DataView(buf);
            const metaLen = view.getUint32(0, true);
            const metaStr = new TextDecoder().decode(new Uint8Array(buf, 4, metaLen));
            grid = JSON.parse(metaStr);
            grid.data = new Float32Array(buf, 4 + metaLen);
        } else {
            const envelope = await response.json();
            if (!envelope.success) {
                console.warn(`⚠️ Grid data not available for ${layerName} in ${region} on ${date}`);
                drawDataBounds(REGION_BOUNDS[region]);
                return;
            }
            grid = envelope.data;
        }
        // API returns compact keys: bounds={n,s,e,w}, size={r,c}
        const bounds = {
            north: grid.bounds.n ?? grid.bounds.north,
            south: grid.bounds.s ?? grid.bounds.south,
            east:  grid.bounds.e ?? grid.bounds.east,
            west:  grid.bounds.w ?? grid.bounds.west,
        };

        // Keep a single bounding box matching actual data extent
        setDataBoundsRect(bounds);

        // Prefer ImageOverlay for categorical flood mask (smooth zoom, no drift).
        // Fallback to canvas overlay for other layers.
        let overlay;
        if (layerName === 'label') {
            overlay = createGridImageOverlay(grid, bounds);
        } else {
            overlay = createGridCanvasOverlay(grid);
        }
        overlay.addTo(map);

        window.activeHeatLayers = window.activeHeatLayers || [];
        window.activeHeatLayers.push(overlay);

        // Cache flood grid for pixel lookup (popup) — đúng bounds từ API
        if (layerName === 'label') {
            const cacheKey = `label_${region}_${date}`;
            _cachedGridData[cacheKey] = {
                data: grid.data,
                width: grid.size.c,
                height: grid.size.r,
                bounds: { north: bounds.north, south: bounds.south, east: bounds.east, west: bounds.west },
            };
        }

    } catch (e) {
        console.error(`❌ Error rendering grid layer ${layerName}:`, e);
        // Fallback: draw static bounds when grid fetch fails
        const rb = REGION_BOUNDS[region];
        if (rb) drawDataBounds(rb);
    }
}

/**
 * Clear all visualize layers (heatmaps)
 */
function clearLayers() {
    // Clear Legacy Heatmap
    if (heatmapLayer) {
        map.removeLayer(heatmapLayer);
        heatmapLayer = null;
    }
    // Clear Flood Mask PNG overlay (if present)
    clearFloodMask();
    clearDataBoundsRect();
    // Clear New Heatmaps
    if (window.activeHeatLayers) {
        window.activeHeatLayers.forEach(l => map.removeLayer(l));
        window.activeHeatLayers = [];
    }
    _cachedGridData = {};
}

/**
 * Cập nhật hiển thị dựa trên date/region và LayerManager
 * Renders BOTH regions simultaneously for complete coverage
 * @param {string} date YYYY-MM-DD
 * @param {string} region DaNang (primary region for LayerManager)
 * @param {boolean} force Force update even if locked (used by toggles)
 */
async function updateHeatmap(date, region, force = false) {
    // 🌍 Update global state immediately so map clicks use correct date
    currentDate = date;
    currentRegion = region;
    if (typeof window !== 'undefined') {
        window.currentDate = date;
        window.currentRegion = region;
    }
    // Cancel any pending pixel requests since date/region changed
    if (_pixelAbortController) {
        _pixelAbortController.abort();
        _pixelAbortController = null;
    }
    _pendingPixelDate = null;

    // --- Lock check ---
    if (isUpdating && !force) {
        console.log(`⏳ updateHeatmap đang chạy, bỏ qua render (nhưng đã set ngày: ${date})`);
        return;
    }
    isUpdating = true;

    console.log(`🗺️ Updating map for: ${region} / ${date} (rendering all regions)`);

    try {
        // Warm-up stacked GeoTIFF source so first pixel click is fast (no downsampling).
        // Non-blocking: we don't await this; it just primes backend caches.
        try {
            fetch(`${window.API_BASE_URL || ''}/api/warmup/${region}/${date}`).catch(() => {});
        } catch {}

        // 1. Update Availability Logic via LayerManager (for the primary/clicked region)
        if (window.LayerManager) {
            await window.LayerManager.updateAvailability(date, region);
            console.log('✅ LayerManager updated availability');
        } else {
            console.warn('⚠️ window.LayerManager is MISSING');
        }

        // 2. Clear old layers
        clearLayers();

        // 3. Get active layers from Manager
        let activeLayers = window.LayerManager ? window.LayerManager.getActiveLayers() : [];
        console.log('📋 Active Layers:', activeLayers);

        if (activeLayers.length === 0) {
            console.warn('⚠️ No active layers! Forcing "label" layer for debugging.');
            activeLayers = [{ layer: 'label', type: 'grid' }];
        }

        // 4. Render ALL regions simultaneously
        const allRegions = ['DaNang'];

        const showFlood = activeLayers.some(l => l.layer === 'label');

        await Promise.all(allRegions.map(async (regionName) => {
            const bounds = REGION_BOUNDS[regionName];
            if (!bounds) return;

            if (showFlood) {
                // Mặc định: grid binary từ /api/grid — bounds + size khớp GeoTIFF → lưới đỏ đúng từng pixel.
                // PNG R2 có thể lệch bounds hoặc quá thô; bật lại bằng window.USE_PNG_FLOOD_MASK = true nếu cần tối ưu mạng.
                if (window.USE_PNG_FLOOD_MASK === true) {
                    const maskOk = await renderFloodMask(regionName, date);
                    if (!maskOk) await renderGridLayer(date, regionName, 'label');
                    else {
                        const rb = REGION_BOUNDS[regionName];
                        if (rb) setDataBoundsRect({ south: rb.south, west: rb.west, north: rb.north, east: rb.east });
                    }
                } else {
                    await renderGridLayer(date, regionName, 'label');
                }
            } else {
                // Fallback: draw static bounds when no grid data is requested
                setDataBoundsRect(bounds);
            }
        }));

    } catch (err) {
        console.error('❌ Error in updateHeatmap:', err);
    } finally {
        isUpdating = false;
    }
}

// Initialize Map
function initMap() {
    console.log('🗺️ Initializing Leaflet map...');

    // --- Define Base Layers ---

    // 1. CartoDB Light (Current) - Gọn, đẹp, load nhanh
    const cartoLight = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: 'abcd',
        maxZoom: 19
    });

    // 2. OpenStreetMap Standard - Chi tiết, có tên đường rõ ràng
    const osm = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        maxZoom: 19
    });

    // 3. Esri World Imagery - Vệ tinh thực tế (Satellite)
    const esriSatellite = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
        maxZoom: 19
    });

    // 4. Google Maps Style (Hybrid/Streets using Google Tiles - Experimental)
    // Note: Google Tiles URL pattern sometimes changes or requires API key, using standard reliable ones above first.

    // --- Create Map ---

    map = L.map('map', {
        zoomControl: false,
        attributionControl: false,
        layers: [osm] // Default: OpenStreetMap (Detailed)
    }).setView([16.1, 108.15], 11);

    // --- Add Base Layer Control ---
    const baseMaps = {
        "Bản đồ Chi tiết (OSM)": osm,
        "Bản đồ Sáng (Light)": cartoLight,
        "Vệ tinh (Satellite)": esriSatellite
    };

    // Add default Leaflet Layer Control (Top Right)
    L.control.layers(baseMaps).addTo(map);

    // --- Expose base layers for toggle button ---
    window._baseLayers = { osm, cartoLight, esriSatellite };
    window._currentBaseKey = 'osm'; // tracks which base is active

    // --- Map Style Toggle Button ---
    const toggleBtn = document.getElementById('map-style-toggle');
    const toggleThumb = document.getElementById('map-style-thumb');
    const toggleLabel = document.getElementById('map-style-label');

    // Thumbnail URLs for preview
    const _thumbSatellite = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/10/460/820';
    const _thumbStreet = 'https://tile.openstreetmap.org/10/820/460.png';

    if (toggleBtn) {
        toggleBtn.addEventListener('click', function () {
            const isSatelliteNow = map.hasLayer(esriSatellite);

            if (isSatelliteNow) {
                // Switch back to street (OSM)
                map.removeLayer(esriSatellite);
                map.addLayer(osm);
                window._currentBaseKey = 'osm';
                toggleThumb.src = _thumbSatellite;
                toggleLabel.textContent = 'Vệ tinh';
                toggleBtn.title = 'Chuyển sang bản đồ vệ tinh';
            } else {
                // Switch to satellite — remove whichever street layer is active
                if (map.hasLayer(osm)) map.removeLayer(osm);
                if (map.hasLayer(cartoLight)) map.removeLayer(cartoLight);
                map.addLayer(esriSatellite);
                window._currentBaseKey = 'esriSatellite';
                toggleThumb.src = _thumbStreet;
                toggleLabel.textContent = 'Bản đồ';
                toggleBtn.title = 'Chuyển sang bản đồ đường';
            }
        });
    }

    // Keep toggle button in sync when user changes layer via the Leaflet control
    map.on('baselayerchange', function (e) {
        if (e.layer === esriSatellite) {
            window._currentBaseKey = 'esriSatellite';
            if (toggleThumb) toggleThumb.src = _thumbStreet;
            if (toggleLabel) toggleLabel.textContent = 'Bản đồ';
        } else {
            window._currentBaseKey = (e.layer === cartoLight) ? 'cartoLight' : 'osm';
            if (toggleThumb) toggleThumb.src = _thumbSatellite;
            if (toggleLabel) toggleLabel.textContent = 'Vệ tinh';
        }
    });

    // --- Add Other Controls & Events ---

    // Event Listeners
    map.on('click', handleMapClick);

    // Initial Data Load
    loadBoundary();

    // --- Tự động lấy ngày mới nhất có trong DB ---
    (async () => {
        try {
            const r = await fetch(`${window.API_BASE_URL}/api/dates/DaNang`);
            const env = await r.json();
            if (env.success && env.data?.availableDates) {
                const years = Object.keys(env.data.availableDates).sort();
                if (years.length > 0) {
                    const y = years[years.length - 1];
                    const months = Object.keys(env.data.availableDates[y]).sort();
                    if (months.length > 0) {
                        const m = months[months.length - 1];
                        const dDays = env.data.availableDates[y][m].sort((a, b) => a - b);
                        if (dDays.length > 0) {
                            const d = dDays[dDays.length - 1];
                            currentDate = `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
                            console.log('📅 Auto-set currentDate from DB:', currentDate);

                            // Sync date-manager UI
                            if (typeof syncDateUI === 'function') syncDateUI(currentDate);
                            const datePicker = document.getElementById('date-picker');
                            if (datePicker) datePicker.value = currentDate;
                            if (typeof selectedCalendarDate !== 'undefined') {
                                selectedCalendarDate = currentDate;
                            }
                        }
                    }
                }
            }
        } catch (e) {
            console.warn('⚠️ Could not auto-set date from DB, using fallback:', currentDate);
        }

        // Fallback if API failed and currentDate is still null
        if (!currentDate) {
            currentDate = '2023-01-17';
            console.warn('⚠️ Using hardcoded fallback date:', currentDate);
        }

        // Initial Heatmap Update (sau khi có date)
        updateHeatmap(currentDate, currentRegion);
        console.log('✅ Map initialized with Layer Control');
    })();
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    console.log('📄 DOM loaded, initializing map...');

    // Check if Leaflet is loaded
    if (typeof L === 'undefined') {
        console.error('❌ Leaflet not loaded');
        return;
    }

    console.log('✅ Leaflet loaded:', L.version);
    console.log('✅ Leaflet.heat loaded:', typeof L.heatLayer !== 'undefined');

    initMap();

    // Export map sau khi initMap() đã chạy (map đã được khởi tạo)
    if (typeof window !== 'undefined') {
        window.updateHeatmap = updateHeatmap;
        // window.map sẽ được set trong initMap() sau khi L.map() tạo xong
    }

    // Initialize layer toggles
    setTimeout(() => {
        if (typeof initLayerToggles === 'function') {
            initLayerToggles();
        }
        // Expose map sau khi đã khởi tạo xong
        if (typeof window !== 'undefined') {
            window.map = map;
        }
    }, 500);
});

// Load Vietnam boundary GeoJSON (Detailed provinces)
async function loadBoundary() {
    try {
        const res = await fetch('/data/vn_geo.json');
        if (!res.ok) throw new Error('Failed to load boundary');
        const geojson = await res.json();

        if (geojson.features && geojson.features.length > 0) {
            vietnamBoundary = geojson; // Lưu toàn bộ FeatureCollection
            console.log(`✅ Loaded Vietnam boundary: ${geojson.features.length} provinces`);
        } else {
            console.warn('⚠️ Invalid GeoJSON structure');
        }
    } catch (e) {
        console.error('❌ Error loading boundary:', e);
    }
}

// Check if point is inside Vietnam territory
function isPointInVietnam(pt) {
    if (!vietnamBoundary || !vietnamBoundary.features) return true; // Nếu chưa load xong thì cứ vẽ

    // Check nếu điểm nằm trong bất kỳ tỉnh nào
    // Tối ưu: Có thể dùng BBox check trước nếu cần thiết, nhưng với số lượng tỉnh này thì loop vẫn ổn cho demo
    for (const feature of vietnamBoundary.features) {
        if (turf.booleanPointInPolygon(pt, feature)) {
            return true;
        }
    }
    return false;
}

/**
 * Helper to draw bounding box of the data region
 * @param {Object} bounds {north, south, east, west}
 */
function drawDataBounds(bounds) {
    if (!bounds) return;

    // We can draw a bounds box if needed using standard Leaflet rect
    // But since no mask layer is used, we just draw directly if needed.
    const boundsRect = L.rectangle(
        [[bounds.south, bounds.west], [bounds.north, bounds.east]],
        { color: '#2196F3', weight: 1, fill: false, dashArray: '5, 5', interactive: false }
    );
    boundsRect.addTo(map);
    console.log('🟦 Added data bounding box for region');
}
