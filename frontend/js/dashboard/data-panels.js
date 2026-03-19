// ─────────────────────────────────────────────────────────
// DATA PANELS MODULE
// Combines Timeline Slider logic (for Map Dashboard) 
// and Detail Charts logic (for Detail Page)
// ─────────────────────────────────────────────────────────

// --- 1. TIMELINE & SLIDER (Dashboard Map Page) ---
let currentDateIndex = 0;
let timelineDates = [];
let isTimelinePlaying = false;
let timelinePlayInterval = null;

async function initTimeline() {
    console.log('⏱️ Initializing timeline component...');
    const slider = document.getElementById('timeline-slider');
    if (!slider) return; // Exit if not on map page

    try {
        const response = await fetch(`${window.API_BASE_URL}/api/timeline`);
        const envelope = await response.json();

        if (!envelope.success) throw new Error(envelope.error?.message || `API error`);

        const data = envelope.data;
        if (data.dates && data.dates.length > 0) {
            timelineDates = data.dates;
        } else {
            timelineDates = generateDateRange(data.dateRange?.start || '2020-01-01', data.dateRange?.end || new Date().toISOString().split('T')[0]);
        }

        slider.max = timelineDates.length - 1;

        const currentDate = window.currentDate || '2023-01-17';
        const startIndex = timelineDates.indexOf(currentDate);
        currentDateIndex = startIndex >= 0 ? startIndex : Math.max(0, timelineDates.length - 1);
        slider.value = currentDateIndex;

        const startElem = document.getElementById('timeline-start');
        const endElem = document.getElementById('timeline-end');
        if (startElem) startElem.textContent = formatTimelineDate(timelineDates[0]);
        if (endElem) endElem.textContent = formatTimelineDate(timelineDates[timelineDates.length - 1]);

        updateTimelineCurrentDateUI();

        if (timelineDates[currentDateIndex]) {
            updateRainfallTrend(timelineDates[currentDateIndex], window.currentRegion || 'DaNang');
        }

        slider.addEventListener('input', handleTimelineSlider);
        const playBtn = document.getElementById('play-btn');
        if (playBtn) playBtn.addEventListener('click', toggleTimelinePlay);

        console.log('✅ Timeline initialized with', timelineDates.length, 'dates');
    } catch (error) {
        console.error('❌ Error initializing timeline:', error);
    }
}

function generateDateRange(startStr, endStr) {
    const result = [];
    const start = new Date(startStr);
    const end = new Date(endStr);
    const step = Math.max(1, Math.floor(((end - start) / 86400000) / 500));
    const current = new Date(start);
    while (current <= end) {
        result.push(current.toISOString().split('T')[0]);
        current.setDate(current.getDate() + step);
    }
    return result;
}

function handleTimelineSlider(e) {
    currentDateIndex = parseInt(e.target.value);
    updateTimelineCurrentDateUI();
    const region = window.currentRegion || 'DaNang';
    const date = timelineDates[currentDateIndex];
    if (typeof updateHeatmap === 'function') updateHeatmap(date, region);
    updateRainfallTrend(date, region);
}

function toggleTimelinePlay() {
    isTimelinePlaying = !isTimelinePlaying;
    const btn = document.getElementById('play-btn');
    if (!btn) return;

    if (isTimelinePlaying) {
        btn.textContent = '⏸ Pause';
        timelinePlayInterval = setInterval(() => {
            currentDateIndex = (currentDateIndex + 1) % timelineDates.length;
            const slider = document.getElementById('timeline-slider');
            if (slider) slider.value = currentDateIndex;
            updateTimelineCurrentDateUI();
            const date = timelineDates[currentDateIndex];
            const region = window.currentRegion || 'DaNang';
            if (typeof updateHeatmap === 'function') updateHeatmap(date, region);
            updateRainfallTrend(date, region);
        }, 2000);
    } else {
        btn.textContent = '▶ Play';
        clearInterval(timelinePlayInterval);
    }
}

function updateTimelineCurrentDateUI() {
    const elem = document.getElementById('current-date');
    if (elem && timelineDates[currentDateIndex]) {
        elem.textContent = formatTimelineDate(timelineDates[currentDateIndex]);
    }
}

function formatTimelineDate(dateStr) {
    if (!dateStr) return '';
    const [year, month, day] = dateStr.split('-').map(Number);
    return new Date(year, month - 1, day).toLocaleDateString('vi-VN', { year: 'numeric', month: 'short', day: 'numeric' });
}

function updateTimelineExternal(dateStr) {
    const region = window.currentRegion || 'DaNang';
    const idx = timelineDates.indexOf(dateStr);
    if (idx >= 0) {
        currentDateIndex = idx;
        const slider = document.getElementById('timeline-slider');
        if (slider) slider.value = idx;
        updateTimelineCurrentDateUI();
    }
    // Always update rainfall trend for selected date (API accepts any date)
    if (dateStr) updateRainfallTrend(dateStr, region);
}

if (typeof window !== 'undefined') {
    window.updateTimeline = updateTimelineExternal;
}

// --- TREND CHARTS (Dashboard Page) ---
let trendChartInstance = null;

function getRiskFromRain(totalRain) {
    if (totalRain >= 80) return 'HIGH';
    if (totalRain >= 30) return 'MEDIUM';
    return 'LOW';
}

function formatForecastLabel(dateStr, baseDateStr) {
    const [year, month, day] = dateStr.split('-').map(Number);
    const d = new Date(year, month - 1, day);
    const short = d.toLocaleDateString('en-US', { month: 'short', day: '2-digit' });
    return dateStr === baseDateStr ? `${short} (Today)` : short;
}

function renderForecastList(trends, baseDateStr) {
    const list = document.getElementById('forecast-list');
    if (!list) return;

    if (!Array.isArray(trends) || trends.length === 0) {
        list.innerHTML = `
            <div class="relative">
                <div class="absolute -left-[21px] top-1 h-3 w-3 rounded-full bg-slate-300 border-2 border-white dark:border-slate-900"></div>
                <div class="flex items-center justify-between">
                    <span class="text-sm font-medium text-slate-600 dark:text-slate-400">No forecast data</span>
                    <span class="text-xs font-bold text-slate-500 bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded">nodata</span>
                </div>
                <p class="text-xs text-slate-400 mt-0.5">API chua tra du lieu du bao</p>
            </div>
        `;
        return;
    }

    list.innerHTML = trends.slice(0, 7).map((item) => {
        const rain = Number(item.total || 0);
        const risk = getRiskFromRain(rain);
        const riskStyles = {
            HIGH: { dot: 'bg-risk-high', badge: 'text-risk-high bg-risk-high/10', text: 'text-risk-high' },
            MEDIUM: { dot: 'bg-risk-med', badge: 'text-risk-med bg-risk-med/10', text: 'text-slate-500' },
            LOW: { dot: 'bg-risk-low', badge: 'text-risk-low bg-risk-low/10', text: 'text-slate-500' },
        }[risk];

        return `
            <div class="relative">
                <div class="absolute -left-[21px] top-1 h-3 w-3 rounded-full ${riskStyles.dot} border-2 border-white dark:border-slate-900"></div>
                <div class="flex items-center justify-between">
                    <span class="text-sm font-medium text-slate-600 dark:text-slate-400">${formatForecastLabel(item.date, baseDateStr)}</span>
                    <span class="text-xs font-bold ${riskStyles.badge} px-2 py-0.5 rounded">${risk}</span>
                </div>
                <p class="text-xs ${riskStyles.text} mt-0.5">Estimated rainfall: ${rain.toFixed(1)} mm</p>
            </div>
        `;
    }).join('');
}

async function updateRainfallTrend(dateStr, region) {
    const canvas = document.getElementById('rainfallTrendChart');
    const hasForecastList = !!document.getElementById('forecast-list');
    if (!canvas && !hasForecastList) return; // Not on dashboard

    try {
        const url = `${window.API_BASE_URL || ''}/api/forecast/${region}/rainfall-trend?date=${dateStr}`;
        const response = await fetch(url);
        const envelope = await response.json();

        if (!envelope.success) throw new Error(envelope.error?.message || 'API Error');

        const trends = envelope.data;
        renderForecastList(trends, dateStr);
        if (!canvas) return;
        if (!Array.isArray(trends) || trends.length === 0) {
            if (trendChartInstance) { trendChartInstance.destroy(); trendChartInstance = null; }
            canvas.style.display = 'none';
            const wrap = canvas.parentElement;
            if (wrap) {
                wrap.style.position = 'relative';
                let nd = document.getElementById('rainfall-trend-nodata');
                if (!nd) {
                    nd = document.createElement('div');
                    nd.id = 'rainfall-trend-nodata';
                    nd.className = 'absolute inset-0 flex items-center justify-center text-xs font-mono text-slate-400';
                    nd.textContent = 'nodata';
                    wrap.appendChild(nd);
                } else nd.style.display = 'flex';
            }
            return;
        }
        canvas.style.display = 'block';
        const ndEl = document.getElementById('rainfall-trend-nodata');
        if (ndEl) ndEl.style.display = 'none';

        const labels = trends.map(t => {
            const parts = t.date.split('-');
            return `${parts[2]}/${parts[1]}`;
        });
        const data = trends.map(t => t.total);

        // Highlight the current date
        const colors = trends.map(t => t.date === dateStr ? '#1976d2' : 'rgba(25, 118, 210, 0.4)');

        if (typeof Chart === 'undefined') {
            console.warn('⏱️ Chart.js not loaded yet. Retrying in 500ms...');
            setTimeout(() => updateRainfallTrend(dateStr, region), 500);
            return;
        }

        if (trendChartInstance && typeof trendChartInstance.update === 'function') {
            trendChartInstance.data.labels = labels;
            trendChartInstance.data.datasets[0].data = data;
            trendChartInstance.data.datasets[0].backgroundColor = colors;

            // Bắt buộc set lại min/max cứng nếu data bằng 0 để tránh scale bóp nát
            const maxVal = Math.max(...data);
            trendChartInstance.options.scales.y.max = maxVal > 0 ? undefined : 50;

            trendChartInstance.update();
        } else {
            const ctx = canvas.getContext('2d');
            const maxVal = Math.max(...data);
            trendChartInstance = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels,
                    datasets: [{
                        label: 'Avg Rainfall (mm)',
                        data,
                        backgroundColor: colors,
                        borderRadius: 4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: function (context) {
                                    return `Avg: ${Number(context.raw).toLocaleString()} mm`;
                                }
                            }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            max: maxVal > 0 ? undefined : 50,
                            ticks: {
                                font: { size: 10 },
                                stepSize: 10
                            }
                        },
                        x: {
                            ticks: { font: { size: 10 } },
                            grid: { display: false }
                        }
                    }
                }
            });
        }
    } catch (e) {
        console.error('Failed to load rainfall trend:', e);
    }
}

// --- 2. DETAIL CHARTS (Detail Page Only) ---
async function initDetailChartsPage() {
    const rainfallChartElem = document.getElementById('rainfallChart');
    if (!rainfallChartElem) return; // Exit if not on detail page

    const urlParams = new URLSearchParams(window.location.search);
    const lat = urlParams.get('lat');
    const lng = urlParams.get('lng');
    const date = urlParams.get('date');
    const region = urlParams.get('region') || 'DaNang';

    if (!lat || !lng || !date) {
        window.location.href = '/';
        return;
    }

    const locElem = document.getElementById('location-text');
    const dateElem = document.getElementById('date-text');
    if (locElem) locElem.textContent = `${parseFloat(lat).toFixed(4)}°N, ${parseFloat(lng).toFixed(4)}°E (${region})`;
    if (dateElem) dateElem.textContent = date;

    try {
        const response = await fetch(`${window.API_BASE_URL || ''}/api/pixel/${lat}/${lng}/${date}/${region}`);
        const data = await response.json();
        updateDetailOverviewCards(data);
        renderDetailCharts(data);
    } catch (e) {
        console.error('Error fetching pixel data detail:', e);
    }
}

function updateDetailOverviewCards(data) {
    const ND = 'nodata';
    const rElem = document.getElementById('rainfall-val');
    const sElem = document.getElementById('soil-val');
    const dElem = document.getElementById('dem-val');
    const riskElem = document.getElementById('risk-val');

    if (rElem) rElem.textContent = data.rainfall != null && !Number.isNaN(Number(data.rainfall)) ? `${data.rainfall} mm` : ND;
    if (sElem) sElem.textContent = data.soilMoisture != null && !Number.isNaN(Number(data.soilMoisture)) ? `${data.soilMoisture}%` : ND;
    if (dElem) dElem.textContent = data.dem != null && !Number.isNaN(Number(data.dem)) ? `${data.dem} m` : ND;
    if (riskElem) {
        const hasRisk = data.floodRisk != null && String(data.floodRisk).trim() !== '';
        riskElem.textContent = hasRisk ? data.floodRisk : ND;
        const colors = { 'LOW': 'text-green-600', 'MEDIUM': 'text-yellow-600', 'HIGH': 'text-orange-600', 'CRITICAL': 'text-red-600' };
        riskElem.className = `text-2xl font-bold mt-1 ${hasRisk ? (colors[data.floodRisk] || 'text-slate-600') : 'text-slate-400'}`;
    }
}

function renderDetailCharts(currentData) {
    const ND = 'nodata';
    const labels = ['-6d', '-5d', '-4d', '-3d', '-2d', '-1d', 'Today'];
    const last = labels.length - 1;
    const rainLast = currentData.rainfall != null && !Number.isNaN(Number(currentData.rainfall)) ? Number(currentData.rainfall) : null;
    const soilLast = currentData.soilMoisture != null && !Number.isNaN(Number(currentData.soilMoisture)) ? Number(currentData.soilMoisture) : null;
    const rainData = labels.map((_, i) => (i === last ? rainLast : null));
    const soilData = labels.map((_, i) => (i === last ? soilLast : null));

    const rCtx = document.getElementById('rainfallChart')?.getContext('2d');
    const rdCtx = document.getElementById('radarChart')?.getContext('2d');
    const cCtx = document.getElementById('correlationChart')?.getContext('2d');
    if (!rCtx || !rdCtx || !cCtx || typeof Chart === 'undefined') return;

    const tooltipFmt = (ctx) => {
        const v = ctx.raw;
        const base = `${ctx.dataset.label}: `;
        return base + (v == null || Number.isNaN(v) ? ND : v);
    };

    new Chart(rCtx, {
        type: 'line',
        data: { labels, datasets: [{ label: 'Rainfall (mm)', data: rainData, borderColor: '#1976d2', backgroundColor: 'rgba(25, 118, 210, 0.1)', tension: 0.4, fill: true, spanGaps: false }] },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { tooltip: { callbacks: { label: tooltipFmt } } }
        }
    });

    const rR = rainLast != null ? Math.min(rainLast, 100) : null;
    const sR = soilLast;
    const sl = currentData.slope != null ? Number(currentData.slope) * 10 : null;
    const el = currentData.dem != null ? Number(currentData.dem) * 5 : null;
    const fl = currentData.flow != null ? Number(currentData.flow) * 2 : null;
    const lc = currentData.landCover != null ? Math.min(Number(currentData.landCover) * 10, 100) : null;
    const radarData = [rR, sR, sl, el, fl, lc];

    new Chart(rdCtx, {
        type: 'radar',
        data: {
            labels: ['Rainfall', 'Soil Moisture', 'Slope', 'Elevation', 'Flow', 'Land Cover'],
            datasets: [{
                label: 'Current Status',
                data: radarData,
                backgroundColor: 'rgba(255, 99, 132, 0.2)', borderColor: 'rgb(255, 99, 132)'
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            scales: { r: { suggestedMax: 100 } },
            plugins: { tooltip: { callbacks: { label: tooltipFmt } } }
        }
    });

    new Chart(cCtx, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: 'Soil Moisture (%)', data: soilData, backgroundColor: 'rgba(255, 87, 34, 0.6)', order: 1, spanGaps: false },
                { label: 'Rainfall (mm)', data: rainData, type: 'line', borderColor: '#2196F3', borderWidth: 2, order: 0, spanGaps: false }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { tooltip: { callbacks: { label: tooltipFmt } } }
        }
    });
}

// --- 3. GLOBAL INIT DISPATCHER ---
document.addEventListener('DOMContentLoaded', () => {
    // Both init functions internally safely check if HTML elements exist
    setTimeout(() => {
        initTimeline();
        initDetailChartsPage();
    }, 1000); // 1s delay to let map.js load first if on dashboard
});
