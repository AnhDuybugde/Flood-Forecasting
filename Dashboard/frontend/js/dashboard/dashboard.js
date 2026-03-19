/**
 * dashboard.js — Pixel Analytics Dashboard Controller
 * Government-grade interactive dashboard with:
 *   - Static terrain KPIs (DEM, Slope, Flow, Land Cover)
 *   - 10-Day interactive timeline (5 past solid + 5 future dashed)
 *   - Seasonality chart (2020–2025, monthly slider)
 *   - Regional statistics table
 *   - Interactive EDA table with heatmap cells
 */

// ─── Chart Instances ───
let chartTimeline = null;
let chartSeasonality = null;

// ─── State ───
const state = {
    lat: 16.05,
    lng: 108.20,
    region: 'DaNang',
    baseDate: null,       // Focus date (string YYYY-MM-DD)
    timelineData: [],     // Array of { date, rain, soil, risk, ... }
    seasonalityData: {},  // { 2020: [12 months], 2021: [...], ... }
    statisticsData: null, // Aggregated stats from pixel history
    focusIndex: 0,        // Which row in timelineData is the focus (last = most recent)
    availableDatesFlat: [], // Sorted array of 'YYYY-MM-DD' strings with data
};

// ─── Helpers ───
function offsetDate(dateStr, days) {
    const d = new Date(dateStr + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + days);
    return d.toISOString().split('T')[0];
}

function fmtDate(dateStr) {
    const [y, m, d] = dateStr.split('-');
    return `${d}/${m}/${y}`;
}

function shortDate(dateStr) {
    return dateStr.substring(5); // MM-DD
}

function $(id) { return document.getElementById(id); }

/** Display token when a value is missing from the database / API */
const UI_NO_DATA = 'nodata';

function isPresentNumber(v) {
    return v != null && v !== '' && typeof v !== 'boolean' && !Number.isNaN(Number(v));
}

/**
 * Map raw pixel API row to timeline row; null = not in DB (show nodata in UI).
 */
function normalizePixelRow(res, date) {
    const row = {
        date,
        isForecast: false,
        rain: null,
        soil: null,
        risk: null,
        dem: null,
        slope: null,
        flow: null,
        landCover: null,
        tide: null,
    };
    if (!res || typeof res !== 'object') return row;
    if (isPresentNumber(res.rainfall)) row.rain = Math.max(0, Number(res.rainfall));
    if (isPresentNumber(res.soilMoisture)) row.soil = Math.max(0, Number(res.soilMoisture) * 100);
    if (res.floodRisk != null && String(res.floodRisk).trim() !== '') {
        row.risk = String(res.floodRisk).toUpperCase();
    }
    if (isPresentNumber(res.dem)) row.dem = Number(res.dem);
    if (isPresentNumber(res.slope)) row.slope = Number(res.slope);
    if (isPresentNumber(res.flow)) row.flow = Number(res.flow);
    if (res.landCover != null && res.landCover !== '' && !Number.isNaN(Number(res.landCover))) {
        row.landCover = Number(res.landCover);
    }
    if (isPresentNumber(res.tide)) row.tide = Number(res.tide);
    return row;
}

// Land cover classification lookup (MODIS IGBP)
const LC_LABELS = {
    1: 'Evergreen Needleleaf', 2: 'Evergreen Broadleaf', 3: 'Deciduous Needleleaf',
    4: 'Deciduous Broadleaf', 5: 'Mixed Forest', 6: 'Closed Shrubland',
    7: 'Open Shrubland', 8: 'Woody Savanna', 9: 'Savanna', 10: 'Grassland',
    11: 'Wetland', 12: 'Cropland', 13: 'Urban', 14: 'Cropland/Natural Mosaic',
    15: 'Snow/Ice', 16: 'Barren', 17: 'Water Bodies'
};

function lcLabel(val) {
    if (val === null || val === undefined || val === '--') return UI_NO_DATA;
    const num = Math.round(Number(val));
    return LC_LABELS[num] || `Class ${num}`;
}

// ─── Statistical mode (most frequent value) ───
function mode(arr) {
    const freq = {};
    arr.forEach(v => { freq[v] = (freq[v] || 0) + 1; });
    let maxCount = 0, maxVal = arr[0];
    for (const [val, count] of Object.entries(freq)) {
        if (count > maxCount) { maxCount = count; maxVal = val; }
    }
    return maxVal;
}

// ─── Risk styling ───
function riskColor(risk) {
    const r = (risk || '').toUpperCase();
    if (r === '' || r === 'NODATA' || r === 'NO DATA' || r === 'NULL') {
        return { bg: 'bg-slate-100', text: 'text-slate-500', border: 'border-slate-200', icon: 'help', iconColor: 'text-slate-400' };
    }
    if (r === 'HIGH' || r === 'CRITICAL') return { bg: 'bg-red-100', text: 'text-red-800', border: 'border-red-300', icon: 'warning', iconColor: 'text-red-500' };
    if (r === 'MEDIUM') return { bg: 'bg-amber-100', text: 'text-amber-800', border: 'border-amber-300', icon: 'visibility', iconColor: 'text-amber-500' };
    return { bg: 'bg-emerald-100', text: 'text-emerald-800', border: 'border-emerald-300', icon: 'verified_user', iconColor: 'text-emerald-500' };
}

// ─── Heatmap class ───
function rainHmClass(v) {
    if (v == null || Number.isNaN(Number(v))) return 'text-slate-400';
    if (v < 5) return 'hm-0';
    if (v < 15) return 'hm-1';
    if (v < 30) return 'hm-2';
    if (v < 50) return 'hm-3';
    return 'hm-4';
}
function soilHmClass(v) {
    if (v == null || Number.isNaN(Number(v))) return 'text-slate-400';
    if (v < 40) return 'hm-0';
    if (v < 60) return 'hm-1';
    if (v < 80) return 'hm-2';
    if (v < 90) return 'hm-3';
    return 'hm-4';
}

// ══════════════════════════════════════════════════════
//  INITIALIZATION
// ══════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', async () => {
    console.log('📊 Dashboard: Initializing...');
    const progress = $('load-progress');
    if (progress) progress.style.width = '20%';

    // Parse URL params (map.js sends lat, lng, date, region)
    const params = new URLSearchParams(window.location.search);
    state.lat = parseFloat(params.get('lat')) || 16.05;
    state.lng = parseFloat(params.get('lng')) || 108.20;
    state.region = params.get('region') || 'DaNang';
    const urlDate = params.get('date') || null; // Respect date from map navigation

    // Update header
    $('header-coords').textContent = `${state.lat.toFixed(3)}°N, ${state.lng.toFixed(3)}°E`;
    $('header-region').textContent = state.region;

    // Load available dates first — needed by all components
    try {
        const datesInfo = await dataLoader.loadAvailableDates(state.region);
        if (datesInfo && datesInfo.availableDates) {
            state.availableDatesFlat = buildFlatDateList(datesInfo.availableDates);
            console.log(`📆 ${state.availableDatesFlat.length} available dates loaded`);
        }
    } catch (e) {
        console.warn('Could not load available dates:', e);
    }

    // Get base date: prefer URL param, then last available date, then fallback
    if (urlDate && /^\d{4}-\d{2}-\d{2}$/.test(urlDate)) {
        state.baseDate = urlDate;
    } else if (state.availableDatesFlat.length > 0) {
        state.baseDate = state.availableDatesFlat[state.availableDatesFlat.length - 1];
    } else {
        try {
            const tl = await dataLoader.loadTimeline();
            state.baseDate = (tl && tl.dateRange && tl.dateRange.end) ? tl.dateRange.end : '2023-01-17';
        } catch (e) {
            console.warn('Timeline fallback:', e);
            state.baseDate = '2023-01-17';
        }
    }

    if (progress) progress.style.width = '40%';

    // Fetch 10 most recent days with available data
    await fetchTimelineData();
    if (progress) progress.style.width = '60%';

    // Fetch regional statistics
    await fetchRegionStats();
    if (progress) progress.style.width = '75%';

    // Render everything
    renderStaticKPIs();
    renderTimelineChart();
    renderRiskKPIs(state.focusIndex);
    renderEDATable();
    if (progress) progress.style.width = '90%';

    // Fetch ML prediction for focus date (non-blocking)
    fetchAndRenderMlPrediction(state.focusIndex);

    // Initialize seasonality (async, non-blocking)
    initSeasonality();

    // Month slider
    setupMonthSlider();

    // Footer sync time
    $('footer-sync-time').textContent = new Date().toLocaleTimeString('vi-VN', {
        hour: '2-digit', minute: '2-digit', second: '2-digit',
        hour12: false, timeZone: 'Asia/Ho_Chi_Minh'
    }) + ' UTC+7';

    // Reveal
    if (progress) progress.style.width = '100%';
    setTimeout(() => {
        const overlay = $('loading-overlay');
        if (overlay) { overlay.style.opacity = '0'; setTimeout(() => overlay.remove(), 500); }
        $('main-content').style.opacity = '1';
    }, 600);

    console.log('✅ Dashboard ready.');
});

// ══════════════════════════════════════════════════════
//  DATA FETCHING
// ══════════════════════════════════════════════════════

/**
 * Build a flat sorted array of 'YYYY-MM-DD' strings from the availableDates nested structure.
 * availableDates format: { "2023": { "01": [1,2,...,31], "02": [...] }, ... }
 */
function buildFlatDateList(avail) {
    const dates = [];
    for (const year of Object.keys(avail).sort()) {
        for (const month of Object.keys(avail[year]).sort()) {
            for (const day of avail[year][month]) {
                dates.push(`${year}-${month}-${String(day).padStart(2, '0')}`);
            }
        }
    }
    return dates.sort();
}

/**
 * Get the N most recent available dates up to and including `fromDate`.
 * Returns dates in chronological order (oldest first).
 */
function getRecentAvailableDates(fromDate, count) {
    const flat = state.availableDatesFlat;
    if (!flat.length) {
        // Fallback: generate consecutive dates ending at fromDate
        const result = [];
        for (let i = -(count - 1); i <= 0; i++) result.push(offsetDate(fromDate, i));
        return result;
    }
    // Find the last date <= fromDate via binary search
    let endIdx = flat.length - 1;
    for (let i = flat.length - 1; i >= 0; i--) {
        if (flat[i] <= fromDate) { endIdx = i; break; }
    }
    const startIdx = Math.max(0, endIdx - count + 1);
    return flat.slice(startIdx, endIdx + 1);
}

async function fetchTimelineData() {
    // Get 10 most recent dates with data, ending at baseDate
    const recentDates = getRecentAvailableDates(state.baseDate, 10);
    console.log(`📊 Timeline: ${recentDates.length} recent dates from ${recentDates[0]} to ${recentDates[recentDates.length - 1]}`);

    const startDate = recentDates[0];
    const endDate = recentDates[recentDates.length - 1];

    // Strategy 1: Try bulk pixel history API (single request)
    let bulkData = null;
    try {
        bulkData = await dataLoader.loadPixelHistory(
            state.lat, state.lng, state.region, startDate, endDate
        );
    } catch (e) {
        console.warn('Bulk pixel history failed, falling back to individual calls:', e);
    }

    if (bulkData && Array.isArray(bulkData) && bulkData.length > 0) {
        // Check if bulk data has any dynamic data (rainfall/soilMoisture)
        const hasDynamicData = bulkData.some(d => isPresentNumber(d.rainfall) || isPresentNumber(d.soilMoisture));

        if (!hasDynamicData) {
            console.info('Bulk history returned only static data — falling back to individual pixel calls');
            bulkData = null; // Force fallback
        }
    }

    // Build a Set of target dates for filtering
    const targetDateSet = new Set(recentDates);

    if (bulkData && Array.isArray(bulkData) && bulkData.length > 0) {
        // Map bulk results to timeline format — only keep dates that are in our target list
        const bulkMap = {};
        bulkData.forEach(d => { if (targetDateSet.has(d.date)) bulkMap[d.date] = d; });

        state.timelineData = [];
        for (const d of recentDates) {
            const res = bulkMap[d];
            state.timelineData.push(normalizePixelRow(res, d));
        }

        // Enrich the focus date (last = most recent) with full pixel data for floodRisk + tide
        try {
            const focusPixel = await dataLoader.loadPixelData(
                state.lat, state.lng, state.baseDate, state.region
            );
            if (focusPixel) {
                const focusEntry = state.timelineData[state.timelineData.length - 1];
                const enriched = normalizePixelRow(focusPixel, state.baseDate);
                if (focusEntry && focusEntry.date === state.baseDate) {
                    if (enriched.rain != null) focusEntry.rain = enriched.rain;
                    if (enriched.soil != null) focusEntry.soil = enriched.soil;
                    if (enriched.risk != null) focusEntry.risk = enriched.risk;
                    if (enriched.tide != null) focusEntry.tide = enriched.tide;
                    if (enriched.dem != null) focusEntry.dem = enriched.dem;
                    if (enriched.slope != null) focusEntry.slope = enriched.slope;
                    if (enriched.flow != null) focusEntry.flow = enriched.flow;
                    if (enriched.landCover != null) focusEntry.landCover = enriched.landCover;
                }
            }
        } catch (e) {
            console.warn('Could not enrich focus date with full pixel data:', e);
        }
    } else {
        // Strategy 2: Fall back to individual pixel calls
        const promises = recentDates.map(d =>
            dataLoader.loadPixelData(state.lat, state.lng, d, state.region)
                .then(res => normalizePixelRow(res, d))
                .catch(() => normalizePixelRow(null, d))
        );
        state.timelineData = await Promise.all(promises);
    }

    // Focus = last item (most recent date)
    state.focusIndex = state.timelineData.length - 1;
}

async function fetchRegionStats() {
    try {
        // Use available dates going backwards from baseDate
        const statsDates = getRecentAvailableDates(state.baseDate, 31);
        const start = statsDates[0];
        const end = statsDates[statsDates.length - 1];
        console.log(`📈 Regional Stats: ${statsDates.length} dates from ${start} to ${end}`);

        const data = await dataLoader.loadPixelHistory(state.lat, state.lng, state.region, start, end);
        if (data && Array.isArray(data) && data.length > 0) {
            // Filter to only include dates that are in our available dates list
            const dateSet = new Set(statsDates);
            const filtered = data.filter(d => dateSet.has(d.date));
            state.statisticsData = filtered;
            renderStatistics(filtered);
        }
    } catch (e) {
        console.warn('Stats fetch error:', e);
    }
}

// ══════════════════════════════════════════════════════
//  RENDER: STATIC KPIs
// ══════════════════════════════════════════════════════

function renderStaticKPIs() {
    // Collect non-null static values from timeline data
    const dems = state.timelineData.map(d => d.dem).filter(v => v !== null);
    const slopes = state.timelineData.map(d => d.slope).filter(v => v !== null);
    const flows = state.timelineData.map(d => d.flow).filter(v => v !== null);
    const lcs = state.timelineData.map(d => d.landCover).filter(v => v !== null);

    const avg = arr => arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1) : UI_NO_DATA;
    const mode = arr => {
        if (!arr.length) return null;
        const freq = {};
        arr.forEach(v => { freq[v] = (freq[v] || 0) + 1; });
        return Object.entries(freq).sort((a, b) => b[1] - a[1])[0][0];
    };

    $('kpi-dem').textContent = avg(dems);
    $('kpi-slope').textContent = avg(slopes);
    $('kpi-flow').textContent = avg(flows);
    $('kpi-landcover').textContent = mode(lcs) != null ? lcLabel(mode(lcs)) : UI_NO_DATA;

    // Also try from stats if available (pixel history uses dem/slope/flow/landCover directly)
    if (state.statisticsData && state.statisticsData.length > 0) {
        const withDem = state.statisticsData.filter(d => d.dem !== null && d.dem !== undefined);
        const withSlope = state.statisticsData.filter(d => d.slope !== null && d.slope !== undefined);
        const withFlow = state.statisticsData.filter(d => d.flow !== null && d.flow !== undefined);
        const withLC = state.statisticsData.filter(d => d.landCover !== null && d.landCover !== undefined);

        if (withDem.length) $('kpi-dem').textContent = (withDem.reduce((a, d) => a + d.dem, 0) / withDem.length).toFixed(1);
        if (withSlope.length) $('kpi-slope').textContent = (withSlope.reduce((a, d) => a + d.slope, 0) / withSlope.length).toFixed(1);
        if (withFlow.length) $('kpi-flow').textContent = (withFlow.reduce((a, d) => a + d.flow, 0) / withFlow.length).toFixed(1);
        if (withLC.length) {
            const lcVals = withLC.map(d => d.landCover);
            const m = mode(lcVals);
            $('kpi-landcover').textContent = m != null ? lcLabel(m) : UI_NO_DATA;
        }
    }
}

// ══════════════════════════════════════════════════════
//  RENDER: RIGHT-SIDE KPI CARDS (focus row)
// ══════════════════════════════════════════════════════

function renderRiskKPIs(idx) {
    const row = state.timelineData[idx];
    if (!row) return;

    // Focus date
    $('kpi-focus-date').textContent = fmtDate(row.date);
    const badge = $('kpi-date-badge');
    badge.textContent = 'OBSERVED';
    badge.className = 'text-[10px] px-2 py-0.5 rounded-full font-semibold bg-slate-100 text-slate-600 border border-slate-200';

    // Rainfall
    const trendEl = $('kpi-rain-trend');
    if (row.rain == null || Number.isNaN(row.rain)) {
        $('kpi-rainfall').textContent = UI_NO_DATA;
        if (trendEl) trendEl.innerHTML = `<span class="text-[10px] text-slate-400">${UI_NO_DATA}</span>`;
    } else {
        $('kpi-rainfall').textContent = row.rain.toFixed(1);
        const prevRow = state.timelineData[idx - 1];
        if (prevRow && trendEl && prevRow.rain != null && !Number.isNaN(prevRow.rain)) {
            const diff = row.rain - prevRow.rain;
            const icon = diff > 0 ? 'trending_up' : diff < 0 ? 'trending_down' : 'trending_flat';
            const color = diff > 5 ? 'text-red-500' : diff < -5 ? 'text-emerald-500' : 'text-slate-400';
            trendEl.innerHTML = `<span class="material-symbols-outlined text-sm ${color}">${icon}</span>
                <span class="text-[10px] ${color} font-semibold">${diff > 0 ? '+' : ''}${diff.toFixed(1)} mm</span>
                <span class="text-[10px] text-slate-400">vs prev day</span>`;
        } else if (trendEl) {
            trendEl.innerHTML = `<span class="text-[10px] text-slate-400">${UI_NO_DATA}</span>`;
        }
    }

    // Soil
    if (row.soil == null || Number.isNaN(row.soil)) {
        $('kpi-soil').textContent = UI_NO_DATA;
        const bar = $('kpi-soil-bar');
        if (bar) { bar.style.width = '0%'; bar.style.background = '#cbd5e1'; }
    } else {
        $('kpi-soil').textContent = row.soil.toFixed(1);
        $('kpi-soil-bar').style.width = Math.min(row.soil, 100) + '%';
        $('kpi-soil-bar').style.background = row.soil > 85 ? '#dc2626' : row.soil > 70 ? '#d97706' : '#059669';
    }

    // Risk
    const riskDisplay = row.risk != null && String(row.risk).trim() !== '' ? row.risk : UI_NO_DATA;
    const rc = riskColor(row.risk);
    $('kpi-risk-text').textContent = riskDisplay;
    $('kpi-risk-text').className = `text-2xl font-extrabold uppercase tracking-wide ${rc.text}`;
    $('kpi-risk-icon').textContent = rc.icon;
    $('kpi-risk-icon').className = `material-symbols-outlined text-2xl ${rc.iconColor}`;
    $('kpi-risk-card').className = `bg-white rounded-xl border-2 p-5 shadow-gov flex-1 ${rc.border}`;
}

// ══════════════════════════════════════════════════════
//  RENDER: ML PREDICTION PANEL
// ══════════════════════════════════════════════════════

/**
 * Fetch and render ML flood prediction for the currently focused date.
 * Called on init and whenever focus changes (click timeline / table row).
 */
async function fetchAndRenderMlPrediction(idx) {
    const row = state.timelineData[idx];
    if (!row) return;

    // Show loading state
    const resultCard = $('ml-result-card');
    const resultRisk = $('ml-result-risk');
    const resultIcon = $('ml-result-icon');
    const resultConf = $('ml-result-confidence');
    if (resultRisk) resultRisk.textContent = '...';
    if (resultConf) resultConf.textContent = '--%';

    const startMs = performance.now();

    try {
        const data = await dataLoader.loadMlPrediction(
            state.lat, state.lng, row.date, state.region
        );
        const latencyMs = Math.round(performance.now() - startMs);

        if (data && data.mlPrediction) {
            renderMlPrediction(data.mlPrediction, latencyMs, row);
        } else {
            // Prediction unavailable — show fallback using rule-based from pixel data
            renderMlFallback(row, latencyMs);
        }
    } catch (e) {
        console.warn('ML prediction rendering failed:', e);
        renderMlFallback(row, 0);
    }
}

/**
 * Render ML prediction results into the ML panel.
 * @param {Object} pred - { flood_risk, probability, confidence, model_version, features_used }
 * @param {number} latencyMs - API call latency
 * @param {Object} row - timeline data row (for feature display)
 */
function renderMlPrediction(pred, latencyMs, row) {
    const risk = (pred.flood_risk || 'LOW').toUpperCase();
    const confidence = pred.confidence ?? 0;
    const probability = pred.probability ?? 0;
    const modelVersion = pred.model_version || 'unknown';

    const rc = riskColor(risk);

    // Result card
    const resultCard = $('ml-result-card');
    if (resultCard) resultCard.className = `flex flex-col items-center justify-center p-6 rounded-xl border-2 ${rc.border} ${rc.bg}`;
    const resultIcon = $('ml-result-icon');
    if (resultIcon) { resultIcon.textContent = rc.icon; resultIcon.className = `material-symbols-outlined text-4xl mb-2 ${rc.iconColor}`; }
    const resultRisk = $('ml-result-risk');
    if (resultRisk) { resultRisk.textContent = risk; resultRisk.className = `text-3xl font-extrabold uppercase tracking-wide ${rc.text}`; }
    const resultConf = $('ml-result-confidence');
    if (resultConf) resultConf.textContent = (confidence * 100).toFixed(0) + '%';

    // Probability bars
    const probNoFlood = 1 - probability;
    const probFlood = probability;
    const barNoFlood = $('ml-bar-noflood');
    const barFlood = $('ml-bar-flood');
    const lblNoFlood = $('ml-prob-noflood');
    const lblFlood = $('ml-prob-flood');
    if (barNoFlood) barNoFlood.style.width = (probNoFlood * 100).toFixed(1) + '%';
    if (barFlood) barFlood.style.width = (probFlood * 100).toFixed(1) + '%';
    if (lblNoFlood) lblNoFlood.textContent = (probNoFlood * 100).toFixed(1) + '%';
    if (lblFlood) lblFlood.textContent = (probFlood * 100).toFixed(1) + '%';

    // Model badge + latency
    const modelBadge = $('ml-model-badge');
    if (modelBadge) {
        modelBadge.textContent = `Model: ${modelVersion}`;
        modelBadge.className = 'text-[10px] font-mono px-2 py-0.5 rounded-full border bg-emerald-50 text-emerald-600 border-emerald-200';
    }
    const latencyBadge = $('ml-latency-badge');
    if (latencyBadge) latencyBadge.textContent = `${latencyMs} ms`;

    // Feature list
    const featureList = $('ml-features-list');
    if (featureList) {
        const features = [
            { label: 'Rainfall', value: row.rain != null ? row.rain.toFixed(1) + ' mm' : UI_NO_DATA, icon: 'water_drop' },
            { label: 'Soil Moisture', value: row.soil != null ? row.soil.toFixed(1) + '%' : UI_NO_DATA, icon: 'grass' },
            { label: 'Tide', value: row.tide != null ? row.tide.toFixed(2) + ' m' : UI_NO_DATA, icon: 'waves' },
            { label: 'DEM', value: row.dem != null ? row.dem.toFixed(1) + ' m' : UI_NO_DATA, icon: 'terrain' },
            { label: 'Slope', value: row.slope != null ? row.slope.toFixed(1) + '°' : UI_NO_DATA, icon: 'landscape' },
            { label: 'Flow Acc', value: row.flow != null ? String(row.flow) : UI_NO_DATA, icon: 'route' },
            { label: 'Land Cover', value: lcLabel(row.landCover), icon: 'forest' },
        ];
        featureList.innerHTML = features.map(f => `
            <div class="flex items-center justify-between py-0.5">
                <span class="flex items-center gap-1.5 text-slate-600">
                    <span class="material-symbols-outlined text-xs text-slate-400">${f.icon}</span>
                    ${f.label}
                </span>
                <span class="font-mono font-medium text-slate-800">${f.value}</span>
            </div>
        `).join('');
    }

    // KPI confidence bar (in the flood risk KPI card)
    const kpiConfBar = $('kpi-confidence-bar');
    const kpiConfVal = $('kpi-confidence-val');
    if (kpiConfBar) {
        const pct = (confidence * 100).toFixed(0);
        kpiConfBar.style.width = pct + '%';
        kpiConfBar.style.background = confidence > 0.8 ? '#059669' : confidence > 0.5 ? '#d97706' : '#dc2626';
    }
    if (kpiConfVal) kpiConfVal.textContent = 'ML ' + (confidence * 100).toFixed(0) + '%';
}

/**
 * Render a fallback when ML service is unavailable.
 * Shows the rule-based risk from pixel data.
 */
function renderMlFallback(row, latencyMs) {
    const risk = (row.risk || 'LOW').toUpperCase();
    const rc = riskColor(risk);

    const resultCard = $('ml-result-card');
    if (resultCard) resultCard.className = `flex flex-col items-center justify-center p-6 rounded-xl border-2 border-slate-200 bg-slate-50`;
    const resultIcon = $('ml-result-icon');
    if (resultIcon) { resultIcon.textContent = 'psychology_alt'; resultIcon.className = 'material-symbols-outlined text-4xl mb-2 text-slate-400'; }
    const resultRisk = $('ml-result-risk');
    if (resultRisk) { resultRisk.textContent = risk + '*'; resultRisk.className = `text-3xl font-extrabold uppercase tracking-wide text-slate-500`; }
    const resultConf = $('ml-result-confidence');
    if (resultConf) resultConf.textContent = 'Rule-based';

    // Clear probability bars
    const barNoFlood = $('ml-bar-noflood');
    const barFlood = $('ml-bar-flood');
    if (barNoFlood) barNoFlood.style.width = '0%';
    if (barFlood) barFlood.style.width = '0%';
    const lblNoFlood = $('ml-prob-noflood');
    const lblFlood = $('ml-prob-flood');
    if (lblNoFlood) lblNoFlood.textContent = UI_NO_DATA;
    if (lblFlood) lblFlood.textContent = UI_NO_DATA;

    // Model badge
    const modelBadge = $('ml-model-badge');
    if (modelBadge) {
        modelBadge.textContent = 'Fallback: Rule-based';
        modelBadge.className = 'text-[10px] font-mono px-2 py-0.5 rounded-full border bg-amber-50 text-amber-600 border-amber-200';
    }
    const latencyBadge = $('ml-latency-badge');
    if (latencyBadge) latencyBadge.textContent = latencyMs > 0 ? `${latencyMs} ms` : '-- ms';

    // Features still shown from pixel data
    const featureList = $('ml-features-list');
    if (featureList) {
        featureList.innerHTML = `
            <div class="text-slate-400 text-[11px] italic py-4 text-center">
                ML service unavailable.<br>Showing rule-based estimation.
            </div>
        `;
    }

    // KPI confidence bar — hide when using fallback
    const kpiConfBar = $('kpi-confidence-bar');
    const kpiConfVal = $('kpi-confidence-val');
    if (kpiConfBar) kpiConfBar.style.width = '0%';
    if (kpiConfVal) kpiConfVal.textContent = UI_NO_DATA;
}

// ══════════════════════════════════════════════════════
//  RENDER: 10-DAY TIMELINE CHART
// ══════════════════════════════════════════════════════

function renderTimelineChart() {
    const ctx = $('chart-timeline');
    if (!ctx) return;

    const parent = ctx.parentElement;
    let nodataEl = $('chart-timeline-nodata');
    const labels = state.timelineData.map(d => shortDate(d.date));
    const rainData = state.timelineData.map(d => (d.rain == null || Number.isNaN(d.rain) ? null : Math.max(0, d.rain)));
    const soilData = state.timelineData.map(d => (d.soil == null || Number.isNaN(d.soil) ? null : Math.max(0, d.soil)));
    const hasAnySeries = rainData.some(v => v != null) || soilData.some(v => v != null);

    if (!hasAnySeries) {
        if (chartTimeline) { chartTimeline.destroy(); chartTimeline = null; }
        ctx.style.display = 'none';
        if (!nodataEl && parent) {
            nodataEl = document.createElement('p');
            nodataEl.id = 'chart-timeline-nodata';
            nodataEl.className = 'absolute inset-0 flex items-center justify-center text-sm font-mono text-slate-400';
            nodataEl.textContent = UI_NO_DATA;
            parent.style.position = 'relative';
            parent.appendChild(nodataEl);
        } else if (nodataEl) nodataEl.style.display = 'flex';
        return;
    }
    ctx.style.display = 'block';
    if (nodataEl) nodataEl.style.display = 'none';

    // Focus date marker (last item = most recent)
    const focusIdx = state.focusIndex;

    const ctxCanvas = ctx.getContext('2d');
    const gradient = ctxCanvas.createLinearGradient(0, 0, 0, 280);
    gradient.addColorStop(0, 'rgba(29, 78, 216, 0.15)');
    gradient.addColorStop(1, 'rgba(29, 78, 216, 0)');

    if (chartTimeline) chartTimeline.destroy();

    chartTimeline = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'Rainfall (mm)',
                    data: rainData,
                    borderColor: '#1d4ed8',
                    backgroundColor: gradient,
                    borderWidth: 2.5,
                    fill: true,
                    tension: 0.3,
                    cubicInterpolationMode: 'monotone',
                    pointBackgroundColor: state.timelineData.map((d, i) => i === state.focusIndex ? '#1d4ed8' : '#ffffff'),
                    pointBorderColor: '#1d4ed8',
                    pointBorderWidth: 2,
                    pointRadius: state.timelineData.map((d, i) => i === state.focusIndex ? 6 : 3),
                    pointHoverRadius: 7,
                    yAxisID: 'y'
                },
                {
                    label: 'Soil Moisture (%)',
                    data: soilData,
                    borderColor: '#d97706',
                    borderWidth: 2,
                    fill: false,
                    tension: 0.3,
                    cubicInterpolationMode: 'monotone',
                    pointRadius: 0,
                    pointHoverRadius: 5,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(15, 23, 42, 0.95)',
                    titleFont: { size: 12, family: 'JetBrains Mono', weight: '700' },
                    bodyFont: { size: 11, family: 'JetBrains Mono' },
                    padding: 14,
                    borderColor: 'rgba(255,255,255,0.08)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    callbacks: {
                        title: items => {
                            const row = state.timelineData[items[0].dataIndex];
                            return `${row.date} (Observed)`;
                        },
                        label: ctx => {
                            const row = state.timelineData[ctx.dataIndex];
                            if (ctx.dataset.yAxisID === 'y') {
                                const v = row?.rain;
                                return `Rainfall (mm): ${v == null ? UI_NO_DATA : Number(v).toFixed(1)}`;
                            }
                            const v = row?.soil;
                            return `Soil Moisture (%): ${v == null ? UI_NO_DATA : Number(v).toFixed(1)}`;
                        }
                    }
                },
                annotation: {
                    annotations: {
                        focusLine: {
                            type: 'line',
                            xMin: focusIdx,
                            xMax: focusIdx,
                            borderColor: 'rgba(29, 78, 216, 0.3)',
                            borderWidth: 2,
                            borderDash: [4, 4],
                            label: {
                                display: true,
                                content: 'FOCUS',
                                position: 'start',
                                backgroundColor: 'rgba(29, 78, 216, 0.85)',
                                font: { size: 9, family: 'JetBrains Mono', weight: '700' },
                                padding: { top: 3, bottom: 3, left: 6, right: 6 },
                                borderRadius: 4,
                            }
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { font: { family: 'JetBrains Mono', size: 10 }, color: '#94a3b8' }
                },
                y: {
                    type: 'linear', display: true, position: 'left',
                    title: { display: true, text: 'Rainfall (mm)', font: { size: 10, family: 'Inter' }, color: '#64748b' },
                    grid: { color: 'rgba(148, 163, 184, 0.1)' },
                    min: 0,
                    ticks: { font: { family: 'JetBrains Mono', size: 10 }, color: '#94a3b8' }
                },
                y1: {
                    type: 'linear', display: true, position: 'right',
                    title: { display: true, text: 'Soil Moisture (%)', font: { size: 10, family: 'Inter' }, color: '#64748b' },
                    grid: { display: false },
                    min: 0, max: 100,
                    ticks: { font: { family: 'JetBrains Mono', size: 10 }, color: '#94a3b8' }
                }
            },
            onClick: (e, elements) => {
                if (elements.length > 0) {
                    const idx = elements[0].index;
                    state.focusIndex = idx;
                    renderRiskKPIs(idx);
                    highlightTableRow(idx);
                    updateTimelineHighlight(idx);
                    fetchAndRenderMlPrediction(idx);
                }
            }
        }
    });
}

function updateTimelineHighlight(idx) {
    if (!chartTimeline) return;
    const ds = chartTimeline.data.datasets[0];
    ds.pointBackgroundColor = state.timelineData.map((_, i) => i === idx ? '#1d4ed8' : '#ffffff');
    ds.pointRadius = state.timelineData.map((_, i) => i === idx ? 6 : 3);
    chartTimeline.update('none');
}

// ══════════════════════════════════════════════════════
//  RENDER: EDA TABLE
// ══════════════════════════════════════════════════════

function renderEDATable() {
    const tbody = $('eda-table-body');
    if (!tbody) return;

    tbody.innerHTML = state.timelineData.map((row, idx) => {
        const isActive = idx === state.focusIndex;
        const activeClass = isActive
            ? 'bg-blue-50/60 ring-2 ring-gov-500/20 ring-inset'
            : 'hover:bg-slate-50';

        const rainCell = row.rain == null || Number.isNaN(row.rain) ? UI_NO_DATA : row.rain.toFixed(1);
        const soilCell = row.soil == null || Number.isNaN(row.soil) ? UI_NO_DATA : row.soil.toFixed(1);
        const riskCell = row.risk != null && String(row.risk).trim() !== '' ? row.risk : UI_NO_DATA;
        const rcRow = riskColor(row.risk);
        return `
        <tr class="cursor-pointer transition-colors ${activeClass}" onclick="onTableRowClick(${idx})" data-row-idx="${idx}">
            <td class="px-6 py-3.5 font-mono text-xs ${isActive ? 'text-gov-500 font-bold' : 'text-slate-700'}">
                ${row.date}
            </td>
            <td class="px-4 py-3.5 text-right data-mono text-xs ${rainHmClass(row.rain)}">${rainCell}</td>
            <td class="px-4 py-3.5 text-right data-mono text-xs ${soilHmClass(row.soil)}">${soilCell}</td>
            <td class="px-4 py-3.5 text-center">
                <span class="text-[10px] font-semibold text-slate-400">OBSERVED</span>
            </td>
            <td class="px-4 py-3.5 text-center">
                <span class="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] font-bold uppercase tracking-wide ${rcRow.bg} ${rcRow.text}">${riskCell}</span>
            </td>
        </tr>`;
    }).join('');
}

function onTableRowClick(idx) {
    state.focusIndex = idx;
    renderRiskKPIs(idx);
    renderEDATable();
    updateTimelineHighlight(idx);
    fetchAndRenderMlPrediction(idx);

    // Sync chart tooltip
    if (chartTimeline) {
        const meta = chartTimeline.getDatasetMeta(0);
        if (meta.data[idx]) {
            chartTimeline.setActiveElements([{ datasetIndex: 0, index: idx }]);
            chartTimeline.tooltip.setActiveElements([{ datasetIndex: 0, index: idx }], { x: meta.data[idx].x, y: meta.data[idx].y });
            chartTimeline.update();
        }
    }
}

function highlightTableRow(idx) {
    renderEDATable(); // Re-render with new active
    const row = document.querySelector(`[data-row-idx="${idx}"]`);
    if (row) row.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

// ══════════════════════════════════════════════════════
//  RENDER: REGIONAL STATISTICS
// ══════════════════════════════════════════════════════

function renderStatistics(histData) {
    if (!histData || histData.length === 0) return;

    // Support both pixel history fields (rainfall, soilMoisture) and region history fields (totalRainfall, avgSoilMoisture)
    const rains = histData.map(d => d.rainfall ?? d.totalRainfall).filter(v => isPresentNumber(v));
    const soils = histData.map(d => d.soilMoisture ?? d.avgSoilMoisture).filter(v => isPresentNumber(v));

    const daysWithRain = rains.filter(r => r > 0);
    const avgRain = daysWithRain.length ? daysWithRain.reduce((a, b) => a + b, 0) / daysWithRain.length : null;
    const maxRain = rains.length ? Math.max(...rains) : null;
    const totalRain = rains.length ? rains.reduce((a, b) => a + b, 0) : null;
    const avgSoil = soils.length ? soils.reduce((a, b) => a + b, 0) / soils.length : null;
    const maxSoil = soils.length ? Math.max(...soils) : null;
    const heavyDays = rains.filter(r => r > 20).length;

    const firstDate = histData[0]?.date || UI_NO_DATA;
    const lastDate = histData[histData.length - 1]?.date || UI_NO_DATA;

    const fmtMm = v => (v == null ? UI_NO_DATA : Number(v).toFixed(1) + ' mm');
    const fmtSoilPct = v => (v == null ? UI_NO_DATA : (Number(v) * 100).toFixed(1) + '%');

    $('stat-avg-rain').textContent = fmtMm(avgRain);
    $('stat-max-rain').textContent = fmtMm(maxRain);
    $('stat-total-rain').textContent = fmtMm(totalRain);
    $('stat-avg-soil').textContent = fmtSoilPct(avgSoil);
    $('stat-max-soil').textContent = fmtSoilPct(maxSoil);
    $('stat-heavy-days').textContent = rains.length ? heavyDays + ' days' : UI_NO_DATA;
    $('stat-period').textContent = `${firstDate} → ${lastDate}`;
    $('stats-region-label').textContent = `${state.region} — ${histData.length} days analyzed`;
}

// ══════════════════════════════════════════════════════
//  SEASONALITY
// ══════════════════════════════════════════════════════

async function initSeasonality() {
    const years = [2020, 2021, 2022, 2023, 2024, 2025];
    const MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    // Initialize datasets with nulls
    years.forEach(y => { state.seasonalityData[y] = new Array(12).fill(null); });

    console.log('📅 Seasonality: Loading monthly rainfall data...');

    // Show loading indicator on chart area
    const chartCanvas = $('chart-seasonality');
    const chartContainer = chartCanvas?.parentElement;
    let loadingOverlay = null;
    if (chartContainer) {
        chartContainer.style.position = 'relative';
        loadingOverlay = document.createElement('div');
        loadingOverlay.id = 'seasonality-loading';
        loadingOverlay.className = 'absolute inset-0 flex items-center justify-center z-10 bg-white/70 rounded-lg';
        loadingOverlay.innerHTML = `
            <div class="flex flex-col items-center gap-2">
                <div class="animate-spin rounded-full h-8 w-8 border-2 border-slate-200 border-t-gov-500"></div>
                <span class="text-xs text-slate-400 font-mono" id="seasonality-loading-text">Loading rainfall data...</span>
            </div>
        `;
        chartContainer.appendChild(loadingOverlay);
    }

    // Render empty chart immediately — single API call for all years (not 6 sequential).
    renderSeasonalityChart(MONTH_LABELS, years);

    const loadingText = $('seasonality-loading-text');
    if (loadingText) loadingText.textContent = 'Loading 2020–2025 (one request)...';

    try {
        const monthlyData = await dataLoader.loadMonthlyRainfall(
            state.lat, state.lng, state.region, years
        );
        if (monthlyData) {
            for (const year of years) {
                const key = String(year);
                if (monthlyData[key]) {
                    state.seasonalityData[year] = monthlyData[key];
                    const monthsWithData = monthlyData[key].filter(v => v !== null).length;
                    console.log(`📅 Seasonality ${year}: ${monthsWithData}/12 months`);
                } else {
                    console.warn(`📅 Seasonality ${year}: no data in response`);
                }
            }
        } else {
            console.warn('📅 Seasonality: monthly endpoint returned empty');
        }
    } catch (e) {
        console.warn(`📅 Seasonality: monthly endpoint failed — ${e.message || e}`);
    }

    if (chartSeasonality) {
        chartSeasonality.data.datasets.forEach((ds, idx) => {
            const y = years[idx];
            if (y !== undefined && state.seasonalityData[y]) {
                ds.data = [...state.seasonalityData[y]];
            }
        });
        chartSeasonality.update('none');
    }

    // Remove loading overlay with fade
    if (loadingOverlay) {
        loadingOverlay.style.transition = 'opacity 0.4s';
        loadingOverlay.style.opacity = '0';
        setTimeout(() => loadingOverlay.remove(), 400);
    }

    console.log('✅ Seasonality chart fully loaded');
}

function renderSeasonalityChart(monthLabels, years) {
    const ctx = $('chart-seasonality');
    if (!ctx) return;

    // Color palette for years (government-appropriate muted palette)
    const yearColors = {
        2020: { border: 'rgba(148, 163, 184, 0.5)', width: 1.5 },
        2021: { border: 'rgba(100, 116, 139, 0.5)', width: 1.5 },
        2022: { border: 'rgba(71, 85, 105, 0.55)', width: 1.5 },
        2023: { border: 'rgba(51, 65, 85, 0.6)', width: 2 },
        2024: { border: 'rgba(30, 41, 59, 0.65)', width: 2 },
        2025: { border: '#d97706', width: 2.5 }, // Amber highlight for most recent
    };

    const datasets = years.map(y => ({
        label: String(y),
        data: [...state.seasonalityData[y]],
        borderColor: yearColors[y].border,
        borderWidth: yearColors[y].width,
        tension: 0.4,
        pointRadius: 0,
        pointHoverRadius: 5,
        spanGaps: true,
    }));

    if (chartSeasonality) chartSeasonality.destroy();

    chartSeasonality = new Chart(ctx, {
        type: 'line',
        data: { labels: monthLabels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    display: true, position: 'bottom',
                    labels: {
                        usePointStyle: true, boxWidth: 8, padding: 16,
                        font: { family: 'Inter', size: 11, weight: '500' },
                        color: '#475569'
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 23, 42, 0.95)',
                    titleFont: { size: 12, family: 'JetBrains Mono', weight: '700' },
                    bodyFont: { size: 11, family: 'JetBrains Mono' },
                    padding: 12,
                    borderColor: 'rgba(255,255,255,0.08)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    callbacks: {
                        label: item => `${item.dataset.label}: ${item.raw !== null && item.raw !== undefined ? item.raw.toFixed(1) + ' mm' : UI_NO_DATA}`
                    }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { font: { family: 'JetBrains Mono', size: 10 }, color: '#94a3b8' }
                },
                y: {
                    grid: { color: 'rgba(148, 163, 184, 0.1)' },
                    title: { display: true, text: 'Monthly Total Rainfall (mm)', font: { size: 10, family: 'Inter' }, color: '#64748b' },
                    min: 0,
                    ticks: { font: { family: 'JetBrains Mono', size: 10 }, color: '#94a3b8' }
                }
            }
        }
    });
}

// ─── Month Slider ───
function setupMonthSlider() {
    const slider = $('month-slider');
    const valLabel = $('month-slider-val');
    const headerLabel = $('month-slider-label');
    const MONTHS = ['All', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    if (!slider) return;

    slider.addEventListener('input', () => {
        const val = parseInt(slider.value);
        valLabel.textContent = MONTHS[val];
        headerLabel.textContent = val === 0 ? 'All Months' : `Focused: ${MONTHS[val]}`;

        if (!chartSeasonality) return;

        if (val === 0) {
            // Show all months - restore full data
            const years = Object.keys(state.seasonalityData).map(Number);
            chartSeasonality.data.datasets.forEach(ds => {
                const y = parseInt(ds.label);
                if (state.seasonalityData[y]) {
                    ds.data = [...state.seasonalityData[y]];
                }
            });
        } else {
            // Highlight single month — show bar-like emphasis
            const monthIdx = val - 1;
            chartSeasonality.data.datasets.forEach(ds => {
                const y = parseInt(ds.label);
                if (state.seasonalityData[y]) {
                    ds.data = state.seasonalityData[y].map((v, i) => i === monthIdx ? v : null);
                }
            });
        }
        chartSeasonality.update();
    });
}

// ══════════════════════════════════════════════════════
//  EXPORT (Basic CSV)
// ══════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
    const btn = $('btn-export');
    if (!btn) return;
    btn.addEventListener('click', () => {
        const headers = ['Date', 'Rainfall_mm', 'SoilMoisture_pct', 'Risk', 'Type'];
        const rows = state.timelineData.map(d =>
            [
                d.date,
                d.rain != null ? d.rain.toFixed(1) : UI_NO_DATA,
                d.soil != null ? d.soil.toFixed(1) : UI_NO_DATA,
                d.risk != null ? d.risk : UI_NO_DATA,
                d.isForecast ? 'Forecast' : 'Observed'
            ].join(',')
        );
        const csv = [headers.join(','), ...rows].join('\n');
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `pixel_${state.lat}_${state.lng}_${state.baseDate}.csv`;
        a.click();
        URL.revokeObjectURL(url);
    });
});
