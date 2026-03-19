/**
 * eda.js — Region EDA with Plotly Heatmaps + Grid API
 *
 * Features:
 *   - URL params (?region=DaNang&date=2020-01-03)
 *   - Date selector from /api/dates/:region or /api/timeline
 *   - Click-to-extract pixel values + Copy to clipboard
 *   - Progress bar, Retry on error, Toast notifications
 *   - Collapse/Expand sections, Layer stats (min/max/mean)
 *   - Keyboard shortcut (Enter = Go), Footer metadata
 */

// ── Clock ──
function updateClock() {
    const now = new Date();
    const el = document.getElementById('nav-clock');
    if (el) el.textContent = now.toLocaleTimeString('en-GB') + ' UTC+7';
}
setInterval(updateClock, 1000);
updateClock();

// ── URL Params ──
const urlParams = new URLSearchParams(window.location.search);
const REGION = urlParams.get('region') || 'DaNang';
let CURRENT_DATE = urlParams.get('date') || '';

// ── Layer Configuration ──
const LAYERS = [
    { id: 'dem', label: 'DEM (m)', colorscale: 'YlGn', unit: 'm', isCat: false },
    { id: 'label', label: 'Flood Risk', colorscale: 'Reds', unit: '', isCat: false },
    { id: 'flow', label: 'Flow Accumulation', colorscale: 'Blues', unit: '', isCat: false },
    { id: 'landCover', label: 'Land Cover Index', colorscale: 'Earth', unit: '', isCat: false },
    { id: 'rain', label: 'Rainfall 24h (mm)', colorscale: 'YlGnBu', unit: 'mm', isCat: false },
    { id: 'soilMoisture', label: 'Soil Moisture (m³/m³)', colorscale: 'YlOrRd', unit: 'm³/m³', isCat: false },
    { id: 'tide', label: 'Tide Level (m)', colorscale: 'RdBu', unit: 'm', isCat: false },
    { id: 'slope', label: 'Slope (°)', colorscale: 'Hot', unit: '°', isCat: false },
];

const gridData = {};
window.LAYERS = LAYERS;
window.gridData = gridData;
let lastLoadTime = null;

// ── UI Helpers ──
function $(id) { return document.getElementById(id); }

function showSkeleton(plotId) {
    const el = $(plotId);
    if (!el) return;
    el.innerHTML = `
        <div class="eda-skeleton">
            <div class="flex flex-col items-center gap-3">
                <span class="material-icons text-slate-300 text-[36px] animate-spin" style="animation-duration:2s">autorenew</span>
                <span class="text-[13px] font-semibold text-slate-500">Loading grid data...</span>
                <div class="w-[180px] flex flex-col gap-2 mt-1">
                    <div class="eda-shimmer-bar w-full"></div>
                    <div class="eda-shimmer-bar w-[85%]"></div>
                    <div class="eda-shimmer-bar w-[60%]"></div>
                </div>
            </div>
        </div>`;
}

function showError(plotId, msg, onRetry) {
    const el = $(plotId);
    if (!el) return;
    el.innerHTML = `
        <div class="eda-skeleton" style="border-color:#fca5a5; background:#fef2f2;">
            <div class="flex flex-col items-center gap-3">
                <span class="material-icons text-red-400 text-[32px]">error_outline</span>
                <span class="text-[13px] font-semibold text-red-500 text-center px-4">${msg}</span>
                ${onRetry ? '<button class="retry-btn flex items-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white text-[12px] font-semibold hover:bg-blue-700 transition"><span class="material-icons text-[18px]">refresh</span> Thử lại</button>' : ''}
            </div>
        </div>`;
    const retryBtn = el.querySelector('.retry-btn');
    if (retryBtn && onRetry) retryBtn.addEventListener('click', onRetry);
}

function setStatusBadge(section, text, color) {
    const badge = section?.querySelector('.status-badge');
    if (badge) {
        badge.textContent = text;
        badge.className = `status-badge text-[11px] font-medium ${color}`;
    }
}

function setStatsBadge(section, stats) {
    const badge = section?.querySelector('.stats-badge');
    if (!badge) return;
    if (!stats || stats.count === 0) {
        badge.classList.add('hidden');
        return;
    }
    badge.textContent = `Min: ${stats.min.toFixed(2)} · Max: ${stats.max.toFixed(2)} · Mean: ${stats.mean.toFixed(2)}`;
    badge.classList.remove('hidden');
}

// ── Toast ──
function toast(msg, type = 'info') {
    const container = $('#toast-container');
    if (!container) return;
    const colors = { success: 'bg-emerald-600 text-white', error: 'bg-red-600 text-white', info: 'bg-slate-800 text-white' };
    const div = document.createElement('div');
    div.className = `eda-toast ${colors[type] || colors.info}`;
    div.textContent = msg;
    container.appendChild(div);
    setTimeout(() => {
        div.style.opacity = '0';
        div.style.transition = 'opacity 0.3s';
        setTimeout(() => div.remove(), 300);
    }, 2500);
}

// ── Progress ──
function updateProgress(current, total, show = true) {
    const wrap = $('#progress-bar-wrap');
    const text = $('#progress-text');
    const pct = $('#progress-pct');
    const fill = $('#progress-fill');
    if (!wrap || !text || !fill) return;
    const percent = total > 0 ? Math.round((current / total) * 100) : 0;
    wrap.classList.toggle('hidden', !show);
    text.textContent = `Đang tải ${current}/${total} layers...`;
    if (pct) pct.textContent = `${percent}%`;
    fill.style.width = `${percent}%`;
}

// ── Collapse ──
function initCollapse() {
    document.querySelectorAll('.eda-section-header').forEach(header => {
        header.addEventListener('click', () => {
            const section = header.closest('.eda-section');
            const body = section?.querySelector('.eda-section-body');
            const icon = header.querySelector('.eda-collapse-icon');
            if (!body) return;
            body.classList.toggle('collapsed');
            if (icon) icon.textContent = body.classList.contains('collapsed') ? 'expand_less' : 'expand_more';
        });
    });
    $('#btn-expand-all')?.addEventListener('click', () => {
        document.querySelectorAll('.eda-section-body').forEach(b => b.classList.remove('collapsed'));
        document.querySelectorAll('.eda-collapse-icon').forEach(i => i.textContent = 'expand_more');
    });
    $('#btn-collapse-all')?.addEventListener('click', () => {
        document.querySelectorAll('.eda-section-body').forEach(b => b.classList.add('collapsed'));
        document.querySelectorAll('.eda-collapse-icon').forEach(i => i.textContent = 'expand_less');
    });
}

// ── Populate Header ──
function updateHeader() {
    const d = CURRENT_DATE || '--';
    const navDate = $('nav-date');
    const headerRegion = $('header-region');
    const headerDate = $('header-date');
    const regionEl = $('nav-region');
    if (navDate) navDate.textContent = d;
    if (headerRegion) headerRegion.textContent = REGION;
    if (headerDate) headerDate.textContent = d;
    if (regionEl) {
        const span = regionEl.querySelector('span:last-child');
        if (span) span.textContent = REGION;
    }
    const footerMeta = $('#footer-meta');
    if (footerMeta) footerMeta.textContent = `Data: GeoTIFF / NPZ · API: Grid binary · Region: ${REGION}`;
}

// ── Load Dates ──
async function loadDates() {
    const select = $('date-select');
    try {
        let dates = [];
        const timelineRes = await fetch('/api/timeline');
        const timelineJson = await timelineRes.json();
        if (timelineJson.success && timelineJson.data?.dates?.length) dates = timelineJson.data.dates;

        if (dates.length === 0) {
            const datesRes = await fetch(`/api/dates/${REGION}`);
            const datesJson = await datesRes.json();
            if (datesJson.success && datesJson.data?.availableDates) {
                const nested = datesJson.data.availableDates;
                for (const [year, months] of Object.entries(nested)) {
                    for (const [month, days] of Object.entries(months)) {
                        for (const day of days) dates.push(`${year}-${month}-${String(day).padStart(2, '0')}`);
                    }
                }
                dates.sort();
            }
        }

        if (dates.length === 0) throw new Error('No dates found');

        select.innerHTML = '';
        dates.forEach(d => {
            const opt = document.createElement('option');
            opt.value = d;
            opt.textContent = d;
            if (d === CURRENT_DATE) opt.selected = true;
            select.appendChild(opt);
        });

        if (!CURRENT_DATE) {
            CURRENT_DATE = dates[dates.length - 1];
            select.value = CURRENT_DATE;
        }
        updateHeader();
    } catch (e) {
        select.innerHTML = '<option value="">No dates available</option>';
        console.error('Failed to load dates:', e);
    }
}

// ── Grid API (binary preferred) ──
async function fetchGrid(layer, date) {
    const url = `/api/grid/${REGION}/${date}/${layer}?format=bin`;
    const t0 = performance.now();
    const res = await fetch(url);
    if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw new Error(body || `HTTP ${res.status}`);
    }
    const contentType = res.headers.get('content-type') || '';
    if (contentType.includes('octet-stream')) {
        const buf = await res.arrayBuffer();
        const view = new DataView(buf);
        const metaLen = view.getUint32(0, true);
        const metaStr = new TextDecoder().decode(new Uint8Array(buf, 4, metaLen));
        const grid = JSON.parse(metaStr);
        // Fix Float32Array offset alignment issue by slicing the ArrayBuffer
        grid.data = new Float32Array(buf.slice(4 + metaLen));
        return grid;
    }
    const json = await res.json();
    if (!json.success || !json.data) throw new Error('Invalid response');
    return json.data;
}

// ── Grid to 2D ──
function gridTo2D(grid) {
    const { data, size, nodata, scale } = grid;
    const rows = size.r, cols = size.c;
    const s = scale || 1, nod = nodata ?? -9999;
    const arr = [];
    for (let r = 0; r < rows; r++) {
        const row = [];
        for (let c = 0; c < cols; c++) {
            const raw = data[r * cols + c];
            row.push((raw === nod || raw <= -9998 || raw == null) ? null : raw / s);
        }
        arr.push(row);
    }
    return arr;
}

// ── Stats from 2D ──
function computeStats(z) {
    let min = Infinity, max = -Infinity, sum = 0, count = 0;
    for (const row of z) {
        for (const v of row) {
            if (v != null && !isNaN(v)) {
                min = Math.min(min, v);
                max = Math.max(max, v);
                sum += v;
                count++;
            }
        }
    }
    return count > 0 ? { min, max, mean: sum / count, count } : null;
}

// ── Render Heatmap ──
function renderHeatmap(plotId, layerCfg, grid) {
    const z = gridTo2D(grid);
    const rows = grid.size.r, cols = grid.size.c;
    const latStep = (grid.bounds.n - grid.bounds.s) / rows;
    const lngStep = (grid.bounds.e - grid.bounds.w) / cols;
    const y = Array.from({ length: rows }, (_, r) => (grid.bounds.n - (r + 0.5) * latStep).toFixed(4));
    const x = Array.from({ length: cols }, (_, c) => (grid.bounds.w + (c + 0.5) * lngStep).toFixed(4));

    const hovertemplate = layerCfg.isCat
        ? 'Lat: %{y}<br>Lng: %{x}<br>Class: %{z}<extra></extra>'
        : `Lat: %{y}<br>Lng: %{x}<br>${layerCfg.label}: %{z:.2f} ${layerCfg.unit}<extra></extra>`;

    Plotly.newPlot(plotId, [{
        z, x, y, type: 'heatmap', colorscale: layerCfg.colorscale, hovertemplate,
        colorbar: { title: { text: layerCfg.unit || layerCfg.label, font: { size: 11 } }, thickness: 15, len: 0.9 },
        zsmooth: 'best',
    }], {
        margin: { l: 70, r: 30, t: 30, b: 55 },
        xaxis: { title: { text: 'Longitude', font: { size: 11 } }, tickfont: { size: 9 }, tickangle: -45, nticks: 12 },
        yaxis: { title: { text: 'Latitude', font: { size: 11 } }, tickfont: { size: 9 }, nticks: 12 },
        height: 420, font: { family: 'Inter, sans-serif' }, paper_bgcolor: 'white', plot_bgcolor: 'white',
    }, { responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d'] });

    const plotEl = $(plotId);
    plotEl.on('plotly_click', function (eventData) {
        if (!eventData.points?.length) return;
        const pt = eventData.points[0];
        const row = pt.pointIndex[0], col = pt.pointIndex[1], val = pt.z;

        const panel = document.querySelector(`.extract-panel[data-layer="${layerCfg.id}"]`);
        if (!panel) return;
        const coordEl = panel.querySelector('.extract-coord');
        const valEl = panel.querySelector('.extract-val');

        coordEl.textContent = `[${row}, ${col}] → Lat ${pt.y}, Lng ${pt.x}`;

        let displayVal = '';
        if (val == null) {
            valEl.innerHTML = '<span class="text-slate-400 italic">nodata</span>';
        } else if (layerCfg.isCat && layerCfg.catLabels) {
            const rounded = Math.round(val);
            const label = layerCfg.catLabels[rounded] || `Class ${rounded}`;
            valEl.innerHTML = `<span class="bg-blue-100 text-blue-800 px-2 py-0.5 rounded text-[12px] font-semibold">${label}</span>`;
            displayVal = label;
        } else {
            valEl.textContent = `${Number(val).toFixed(3)} ${layerCfg.unit}`;
            displayVal = `${Number(val).toFixed(3)} ${layerCfg.unit}`;
        }

        let copyBtn = panel.querySelector('.btn-copy-extract');
        if (!copyBtn) {
            copyBtn = document.createElement('button');
            copyBtn.className = 'btn-copy-extract inline-flex items-center gap-1 px-2 py-1 rounded bg-blue-50 text-blue-600 hover:bg-blue-100 text-[11px] font-medium transition';
            copyBtn.title = 'Copy value';
            copyBtn.innerHTML = '<span class="material-icons text-[14px]">content_copy</span> Copy';
            panel.appendChild(copyBtn);
        }
        copyBtn.classList.remove('hidden');
        copyBtn.dataset.copyVal = displayVal || String(val);
        copyBtn.onclick = () => {
            navigator.clipboard.writeText(copyBtn.dataset.copyVal || '');
            toast('Đã copy vào clipboard', 'success');
        };

        valEl.parentElement?.animate?.({ backgroundColor: ['#f1f5f9', 'transparent'] }, { duration: 400, fill: 'forwards' });
    });
}

// ── Load All Layers ──
async function loadAllLayers(date) {
    const total = LAYERS.length;
    updateProgress(0, total, true);
    lastLoadTime = null;
    const t0 = performance.now();

    let loadedCount = 0;
    
    // Create an array of Promises so all requests fire simultaneously
    const fetchPromises = LAYERS.map(async (layerCfg) => {
        const plotId = 'plot-' + layerCfg.id;
        const section = document.querySelector(`section[data-layer="${layerCfg.id}"]`);

        showSkeleton(plotId);
        if (section) setStatusBadge(section, 'Loading...', 'text-blue-500');

        try {
            const grid = await fetchGrid(layerCfg.id, date);
            gridData[layerCfg.id] = grid;
            const z = gridTo2D(grid);
            renderHeatmap(plotId, layerCfg, grid);

            if (section) {
                const cells = grid.size.r * grid.size.c;
                setStatusBadge(section, `${grid.size.r}×${grid.size.c} = ${cells.toLocaleString()} cells`, 'text-emerald-600');
                const stats = computeStats(z);
                if (stats && !layerCfg.isCat) setStatsBadge(section, stats);
            }
        } catch (e) {
            console.error(`Layer ${layerCfg.id} failed:`, e);
            showError(plotId, `Failed: ${e.message}`, () => {
                // For retry, we just re-run this specific layer logic, not the whole thing
                loadAllLayers(date); 
            });
            if (section) setStatusBadge(section, 'Error', 'text-red-500');
        } finally {
            loadedCount++;
            updateProgress(loadedCount, total, true);
        }
    });

    await Promise.all(fetchPromises);

    lastLoadTime = ((performance.now() - t0) / 1000).toFixed(1);
    setTimeout(() => $('#progress-bar-wrap')?.classList.add('hidden'), 600);
    const footerTime = $('#footer-load-time');
    if (footerTime) footerTime.textContent = `Load: ${lastLoadTime}s`;
    toast(`Đã tải xong ${total} layers`, 'success');
    
    // Trigger custom event for DataTables visualization
    document.dispatchEvent(new CustomEvent('edaDataLoaded', { detail: { date } }));
}

// ── Date Change ──
function applyDateChange() {
    const newDate = $('date-select')?.value;
    if (!newDate) return;
    CURRENT_DATE = newDate;
    updateHeader();
    const url = new URL(window.location);
    url.searchParams.set('date', newDate);
    url.searchParams.set('region', REGION);
    window.history.replaceState({}, '', url);
    loadAllLayers(CURRENT_DATE);
}

// ── Boot ──
(async function boot() {
    updateHeader();
    initCollapse();

    $('#btn-go')?.addEventListener('click', applyDateChange);
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.target.matches('input, textarea')) applyDateChange();
    });

    await loadDates();
    if (CURRENT_DATE) loadAllLayers(CURRENT_DATE);
})();
