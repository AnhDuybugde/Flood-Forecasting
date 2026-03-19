/**
 * eda-app.js — Core application logic for the new EDA system.
 * Handles sidebar navigation, date loading, grid data fetching, shared utilities.
 */
'use strict';

// ── Shared State ──
window.EDA = {
    region: 'DaNang',
    date: null,
    gridData: {},
    layers: [],
    loaded: false,
};

// ── Layer Configuration ──
const LAYERS = [
    { id: 'rain',         label: 'Precipitation',   unit: 'mm',   color: '#38bdf8', icon: 'water_drop',         isCat: false },
    { id: 'soilMoisture', label: 'Soil Moisture',    unit: 'Vol%', color: '#f97316', icon: 'grass',              isCat: false },
    { id: 'tide',         label: 'Tide Level',       unit: 'm',    color: '#06b6d4', icon: 'sailing',            isCat: false },
    { id: 'dem',          label: 'DEM',              unit: 'm',    color: '#34d399', icon: 'terrain',            isCat: false },
    { id: 'slope',        label: 'Slope',            unit: '°',    color: '#f43f5e', icon: 'signal_cellular_alt',isCat: false },
    { id: 'flow',         label: 'Flow Accum.',      unit: '',     color: '#818cf8', icon: 'waves',              isCat: false },
    { id: 'landCover',    label: 'Land Cover',       unit: '',     color: '#fbbf24', icon: 'forest',             isCat: true  },
    { id: 'label',        label: 'Flood Label',      unit: '',     color: '#f87171', icon: 'warning_amber',      isCat: true  },
];
window.LAYERS = LAYERS;

// ── Plotly Config ──
window.PLOTLY_CFG = {
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ['lasso2d', 'select2d'],
    displaylogo: false,
};

window.darkLayout = function(title, overrides = {}) {
    return {
        title: { text: title, font: { size: 13, family: 'Inter, sans-serif', color: '#e2e8f0' } },
        font: { family: 'Inter, sans-serif', size: 11, color: '#94a3b8' },
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(15,23,42,0.5)',
        margin: { l: 55, r: 25, t: 44, b: 45 },
        xaxis: { gridcolor: 'rgba(51,65,85,0.4)', zerolinecolor: 'rgba(51,65,85,0.6)', ...overrides.xaxis },
        yaxis: { gridcolor: 'rgba(51,65,85,0.4)', zerolinecolor: 'rgba(51,65,85,0.6)', ...overrides.yaxis },
        ...overrides,
    };
};

// ── Grid Value Extraction ──
window.gridVal = function(grid, idx) {
    const raw = grid.data[idx];
    const nodata = grid.nodata ?? -9999;
    if (raw === nodata || raw <= -9998 || raw == null) return null;
    return raw / (grid.scale || 1);
};

window.sampleIdx = function(n = 1200) {
    const keys = Object.keys(window.EDA.gridData);
    if (!keys.length) return [];
    const grid = window.EDA.gridData[keys[0]];
    const total = grid.data.length;
    const indices = [];
    const step = Math.max(1, Math.floor(total / n));
    for (let i = 0; i < total && indices.length < n; i += step) indices.push(i);
    return indices;
};

window.extractValues = function(layerId, max = 0) {
    const grid = window.EDA.gridData[layerId];
    if (!grid) return [];
    const result = [];
    const step = max > 0 ? Math.max(1, Math.floor(grid.data.length / max)) : 1;
    for (let i = 0; i < grid.data.length; i += step) {
        const v = gridVal(grid, i);
        if (v !== null) result.push(v);
    }
    return result;
};

window.layerColor = id => LAYERS.find(l => l.id === id)?.color || '#64748b';
window.layerLabel = id => LAYERS.find(l => l.id === id)?.label || id;
window.layerUnit  = id => LAYERS.find(l => l.id === id)?.unit  || '';

// ── Toast ──
window.toast = function(msg, type = 'info') {
    const el = document.createElement('div');
    el.className = `toast toast-${type}`;
    el.textContent = msg;
    document.getElementById('toast-container').appendChild(el);
    setTimeout(() => el.remove(), 3500);
};

// ── Clock ──
function updateClock() {
    const now = new Date();
    const s = now.toLocaleTimeString('en-GB', { timeZone: 'Asia/Ho_Chi_Minh', hour12: false });
    const el = document.getElementById('nav-clock');
    if (el) el.textContent = s + ' UTC+7';
}
setInterval(updateClock, 1000);
updateClock();

// ── Sidebar Navigation ──
document.querySelectorAll('.nav-item[data-page]').forEach(item => {
    item.addEventListener('click', () => {
        document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
        document.querySelectorAll('.subpage').forEach(p => p.classList.remove('active'));
        item.classList.add('active');
        const page = document.getElementById('page-' + item.dataset.page);
        if (page) page.classList.add('active');
        const title = document.getElementById('topbar-title');
        if (title) title.textContent = item.textContent.trim();
        setTimeout(() => window.dispatchEvent(new Event('resize')), 60);
    });
});

// ── Table Tabs ──
document.querySelectorAll('#eda-table-tabs .table-tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('#eda-table-tabs .table-tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.ttab-content').forEach(c => c.classList.remove('active'));
        tab.classList.add('active');
        const target = document.getElementById(tab.dataset.target);
        if (target) target.classList.add('active');
    });
});

// ── Date Loading ──
async function loadDates() {
    try {
        const res = await fetch(`/api/timeline`);
        if (!res.ok) throw new Error('Timeline API error');
        const json = await res.json();
        const dates = json.data?.dates || json.dates || [];
        window.EDA.allDates = dates;
        const sel = document.getElementById('date-select');
        if (!sel) return;
        sel.innerHTML = '';
        dates.forEach(d => {
            const opt = document.createElement('option');
            opt.value = d; opt.textContent = d;
            sel.appendChild(opt);
        });
        // Populate diff-date-b select for spatial difference map
        const diffSel = document.getElementById('diff-date-b');
        if (diffSel) { diffSel.innerHTML = ''; dates.forEach(d => { const o = document.createElement('option'); o.value = d; o.textContent = d; diffSel.appendChild(o); }); }
        if (dates.length) {
            sel.value = dates[dates.length - 1];
            window.EDA.date = sel.value;
        }
    } catch (e) {
        console.error('[EDA] loadDates error', e);
        toast('Không thể tải danh sách ngày', 'error');
    }
}

// ── Grid Binary Loader ──
async function loadGridBin(region, date, layer) {
    const url = `/api/grid/${region}/${date}/${layer}?format=bin`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Grid ${layer} failed: ${res.status}`);
    const buf = await res.arrayBuffer();
    const view = new DataView(buf);
    const metaLen = view.getUint32(0, true);
    const metaStr = new TextDecoder().decode(new Uint8Array(buf, 4, metaLen));
    const meta = JSON.parse(metaStr);
    const dataOffset = 4 + metaLen;
    // Float32Array requires offset aligned to 4 bytes; copy if unaligned
    const dataBuf = buf.slice(dataOffset);
    const f32 = new Float32Array(dataBuf);
    return { ...meta, data: f32 };
}

async function loadAllGrids() {
    const region = window.EDA.region;
    const date = window.EDA.date;
    if (!date) { toast('Chưa chọn ngày', 'error'); return; }

    const progressWrap = document.getElementById('progress-wrap');
    const progressText = document.getElementById('progress-text');
    const progressPct  = document.getElementById('progress-pct');
    const progressFill = document.getElementById('progress-fill');
    progressWrap.style.display = 'block';

    const layerIds = LAYERS.map(l => l.id);
    const total = layerIds.length;
    let loaded = 0;
    window.EDA.gridData = {};

    const t0 = performance.now();

    for (const lid of layerIds) {
        try {
            const grid = await loadGridBin(region, date, lid);
            window.EDA.gridData[lid] = grid;
        } catch (e) {
            console.warn(`[EDA] Grid ${lid} failed:`, e.message);
        }
        loaded++;
        const pct = Math.round((loaded / total) * 100);
        progressText.textContent = `Đang tải ${loaded}/${total} layers...`;
        progressPct.textContent = pct + '%';
        progressFill.style.width = pct + '%';
    }

    const elapsed = ((performance.now() - t0) / 1000).toFixed(1);
    progressWrap.style.display = 'none';
    window.EDA.loaded = true;

    const ftEl = document.getElementById('footer-load-time');
    if (ftEl) ftEl.textContent = `Loaded in ${elapsed}s`;

    toast(`✅ Đã tải ${loaded}/${total} layers (${elapsed}s)`, 'success');

    // Dispatch event for sub-page modules
    document.dispatchEvent(new CustomEvent('edaDataReady'));
}

// ── Go Button ──
document.getElementById('btn-go')?.addEventListener('click', () => {
    const sel = document.getElementById('date-select');
    window.EDA.date = sel?.value;
    loadAllGrids();
});

// ── Auto-load on date change ──
document.getElementById('date-select')?.addEventListener('change', (e) => {
    window.EDA.date = e.target.value;
});

// ── Init ──
(async function init() {
    await loadDates();
    loadAllGrids();
})();
