/**
 * eda-charts.js — Advanced Interactive Charts for EDA Sub-Pages
 * 
 * Sub-pages:
 *   - distribution: Histogram, Box Plot, Violin
 *   - correlation:  Scatter XY, Bubble, Correlation Heatmap
 *   - categorical:  Pie/Donut Flood, LULC Bar, Treemap
 *   - advanced:     Parallel Coordinates, Radar
 * 
 * All charts use data from window.gridData (loaded by eda.js)
 */

'use strict';

/** Shown when chart series has no values from the grid / DB */
const CHART_NO_DATA = 'nodata';
const chartPlaceholder = () => `<p class="text-slate-400 text-center p-8">${CHART_NO_DATA}</p>`;

// ── State ──
const EDACharts = {
    initialized: false,
    chartInstances: {},
    sampleSize: 1200,
};

// ── Utilities ──
function gridValAt(grid, idx) {
    const raw = grid.data[idx];
    const nodata = grid.nodata ?? -9999;
    if (raw === nodata || raw <= -9998 || raw == null) return null;
    return raw / (grid.scale || 1);
}

/**
 * Sample up to `n` random valid pixel indices from the first available grid.
 * Returns array of index numbers.
 */
function sampleIndices(n = EDACharts.sampleSize) {
    const keys = Object.keys(window.gridData || {});
    if (!keys.length) return [];
    const grid = window.gridData[keys[0]];
    const total = grid.data.length;
    const indices = [];
    const step = Math.max(1, Math.floor(total / n));
    for (let i = 0; i < total && indices.length < n; i += step) {
        indices.push(i);
    }
    return indices;
}

/** Extract flat valid values from a layer */
function extractValues(layerId) {
    const grid = window.gridData?.[layerId];
    if (!grid) return [];
    const result = [];
    for (let i = 0; i < grid.data.length; i++) {
        const v = gridValAt(grid, i);
        if (v !== null) result.push(v);
    }
    return result;
}

/** Normalize an array to [0, 1] */
function normalize(arr) {
    const min = Math.min(...arr), max = Math.max(...arr);
    const range = max - min || 1;
    return arr.map(v => (v - min) / range);
}

/** Compute mean of an array */
function mean(arr) {
    if (!arr.length) return 0;
    return arr.reduce((a, b) => a + b, 0) / arr.length;
}

/** Dark modern Plotly layout base */
function baseLayout(title, overrides = {}) {
    return {
        title: { text: title, font: { size: 14, family: 'Inter, sans-serif', color: '#1e293b' } },
        font: { family: 'Inter, sans-serif', size: 11 },
        paper_bgcolor: '#fff',
        plot_bgcolor: '#f8fafc',
        margin: { l: 60, r: 30, t: 48, b: 50 },
        ...overrides,
    };
}

const PLOTLY_CONFIG = {
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ['lasso2d', 'select2d', 'toImage'],
    displaylogo: false,
};

// ── Color Map ──
const LAYER_COLORS = {
    dem: '#10b981',
    label: '#ef4444',
    flow: '#6366f1',
    landCover: '#f59e0b',
    rain: '#0ea5e9',
    soilMoisture: '#f97316',
    tide: '#06b6d4',
    slope: '#f43f5e',
};

function layerColor(id) { return LAYER_COLORS[id] || '#64748b'; }
function layerLabel(id) {
    return window.LAYERS?.find(l => l.id === id)?.label || id;
}
function layerUnit(id) {
    return window.LAYERS?.find(l => l.id === id)?.unit || '';
}

// ======================================================
// DISTRIBUTION CHARTS
// ======================================================

function renderHistogram(layerId, bins = 40) {
    const el = document.getElementById('chart-histogram');
    if (!el) return;
    const values = extractValues(layerId);
    if (!values.length) { el.innerHTML = chartPlaceholder(); return; }

    Plotly.newPlot(el, [{
        x: values,
        type: 'histogram',
        nbinsx: bins,
        marker: { color: layerColor(layerId), opacity: 0.85, line: { color: '#fff', width: 0.5 } },
        name: layerLabel(layerId),
        hovertemplate: `Value: %{x:.3f}<br>Count: %{y}<extra></extra>`,
    }], baseLayout(`Histogram — ${layerLabel(layerId)}`, {
        xaxis: { title: { text: `${layerLabel(layerId)} ${layerUnit(layerId) ? '('+layerUnit(layerId)+')' : ''}`, font: { size: 11 } } },
        yaxis: { title: { text: 'Pixel Count', font: { size: 11 } } },
        height: 380,
        bargap: 0.08,
    }), PLOTLY_CONFIG);
}

function renderBoxPlot() {
    const el = document.getElementById('chart-boxplot');
    if (!el) return;
    const layers = Object.keys(window.gridData || {});
    if (!layers.length) return;

    const traces = layers.map(id => {
        // sample 3000 values max for performance
        const grid = window.gridData[id];
        const vals = [];
        const step = Math.max(1, Math.floor(grid.data.length / 3000));
        for (let i = 0; i < grid.data.length && vals.length < 3000; i += step) {
            const v = gridValAt(grid, i);
            if (v !== null) vals.push(v);
        }
        return {
            y: vals,
            type: 'box',
            name: layerLabel(id),
            marker: { color: layerColor(id), size: 3 },
            boxpoints: 'outliers',
            jitter: 0.3,
            hovertemplate: `%{y:.3f}<extra>${layerLabel(id)}</extra>`,
        };
    });

    if (!traces.some(t => t.y && t.y.length)) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    Plotly.newPlot(el, traces, baseLayout('Box Plot — Tất Cả Layers (Raw Values)', {
        height: 420,
        yaxis: { title: { text: 'Giá trị gốc', font: { size: 11 } } },
        showlegend: false,
        boxmode: 'group',
    }), PLOTLY_CONFIG);
}

function renderViolinPlot(layerId) {
    const el = document.getElementById('chart-violin');
    if (!el) return;
    const grid = window.gridData?.[layerId];
    if (!grid) return;
    const vals = [];
    const step = Math.max(1, Math.floor(grid.data.length / 3000));
    for (let i = 0; i < grid.data.length && vals.length < 3000; i += step) {
        const v = gridValAt(grid, i);
        if (v !== null) vals.push(v);
    }

    if (!vals.length) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    Plotly.newPlot(el, [{
        y: vals,
        type: 'violin',
        name: layerLabel(layerId),
        box: { visible: true },
        meanline: { visible: true, color: '#1e293b', width: 2 },
        fillcolor: layerColor(layerId),
        opacity: 0.75,
        line: { color: layerColor(layerId) },
        hovertemplate: `%{y:.3f}<extra></extra>`,
    }], baseLayout(`Violin Plot — ${layerLabel(layerId)}`, {
        height: 380,
        yaxis: { title: { text: layerLabel(layerId), font: { size: 11 } } },
        showlegend: false,
    }), PLOTLY_CONFIG);
}

// ======================================================
// CORRELATION & SCATTER CHARTS
// ======================================================

function renderScatter(xId, yId) {
    const el = document.getElementById('chart-scatter');
    if (!el) return;
    const indices = sampleIndices(1500);
    const xGrid = window.gridData?.[xId], yGrid = window.gridData?.[yId];
    const labelGrid = window.gridData?.['label'];
    if (!xGrid || !yGrid) return;

    const xVals = [], yVals = [], colors = [];
    for (const idx of indices) {
        const xv = gridValAt(xGrid, idx), yv = gridValAt(yGrid, idx);
        if (xv === null || yv === null) continue;
        xVals.push(xv);
        yVals.push(yv);
        const lv = labelGrid ? gridValAt(labelGrid, idx) : 0;
        colors.push(lv > 0 ? '#ef4444' : '#3b82f6');
    }

    if (!xVals.length) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    Plotly.newPlot(el, [{
        x: xVals, y: yVals,
        mode: 'markers',
        type: 'scatter',
        marker: { color: colors, size: 5, opacity: 0.65, line: { color: '#fff', width: 0.3 } },
        hovertemplate: `${layerLabel(xId)}: %{x:.3f}<br>${layerLabel(yId)}: %{y:.3f}<extra></extra>`,
    }], baseLayout(`Scatter — ${layerLabel(xId)} vs ${layerLabel(yId)}`, {
        height: 400,
        xaxis: { title: { text: `${layerLabel(xId)} (${layerUnit(xId)})`, font: { size: 11 } } },
        yaxis: { title: { text: `${layerLabel(yId)} (${layerUnit(yId)})`, font: { size: 11 } } },
        annotations: [{
            text: '🔴 Flood  🔵 Normal',
            xref: 'paper', yref: 'paper', x: 1, y: 1.04,
            showarrow: false, font: { size: 10, color: '#64748b' }, xanchor: 'right',
        }]
    }), PLOTLY_CONFIG);
}

function renderBubbleChart() {
    const el = document.getElementById('chart-bubble');
    if (!el) return;
    const indices = sampleIndices(800);
    const demGrid = window.gridData?.dem;
    const rainGrid = window.gridData?.rain;
    const flowGrid = window.gridData?.flow;
    const labelGrid = window.gridData?.label;
    if (!demGrid || !rainGrid) { el.innerHTML = chartPlaceholder(); return; }

    const x = [], y = [], sizes = [], colors = [], texts = [];
    for (const idx of indices) {
        const dem = gridValAt(demGrid, idx);
        const rain = gridValAt(rainGrid, idx);
        if (dem === null || rain === null) continue;
        const flow = flowGrid ? (gridValAt(flowGrid, idx) || 0) : 1;
        const lbl = labelGrid ? (gridValAt(labelGrid, idx) || 0) : 0;
        x.push(dem); y.push(rain);
        sizes.push(Math.max(4, Math.min(30, Math.log1p(Math.abs(flow)) * 2)));
        colors.push(lbl > 0 ? '#ef4444' : '#3b82f6');
        texts.push(`DEM: ${dem.toFixed(1)}m<br>Rain: ${rain.toFixed(1)}mm<br>Flow: ${flow.toFixed(0)}<br>Flood: ${lbl > 0 ? 'Yes' : 'No'}`);
    }

    if (!x.length) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    Plotly.newPlot(el, [{
        x, y, mode: 'markers', type: 'scatter',
        text: texts, hoverinfo: 'text',
        marker: { color: colors, size: sizes, opacity: 0.7, line: { color: '#fff', width: 0.5 } },
    }], baseLayout('Bubble Chart — DEM vs Rain (size=Flow, color=Flood)', {
        height: 420,
        xaxis: { title: { text: 'Độ Cao DEM (m)', font: { size: 11 } } },
        yaxis: { title: { text: 'Lượng Mưa 24h (mm)', font: { size: 11 } } },
        annotations: [{
            text: '🔴 Flood  🔵 Normal  •  Bubble size = Log(Flow)',
            xref: 'paper', yref: 'paper', x: 0.5, y: -0.13,
            showarrow: false, font: { size: 10, color: '#64748b' }, xanchor: 'center',
        }]
    }), PLOTLY_CONFIG);
}

function renderCorrelationHeatmap() {
    const el = document.getElementById('chart-corr-heatmap');
    if (!el) return;
    const numericLayers = (window.LAYERS || []).filter(l => !l.isCat).map(l => l.id);
    const n = numericLayers.length;
    if (n === 0) return;

    // ── Pearson correlation matrix ──
    const zMatrix = [];
    const labels = numericLayers.map(id => layerLabel(id));

    for (let i = 0; i < n; i++) {
        const row = [];
        for (let j = 0; j < n; j++) {
            if (i === j) { row.push(1); continue; }
            const r = getPearsonFast(window.gridData[numericLayers[i]], window.gridData[numericLayers[j]]);
            row.push(r == null ? null : r);
        }
        zMatrix.push(row);
    }

    // Annotations
    const annots = [];
    for (let i = 0; i < n; i++) {
        for (let j = 0; j < n; j++) {
            const v = zMatrix[i][j];
            const isNum = typeof v === 'number' && !Number.isNaN(v);
            annots.push({
                x: labels[j], y: labels[i],
                text: isNum ? v.toFixed(2) : CHART_NO_DATA,
                showarrow: false,
                font: { size: 10, color: isNum && Math.abs(v) > 0.5 ? '#fff' : '#64748b' }
            });
        }
    }

    Plotly.newPlot(el, [{
        z: zMatrix, x: labels, y: labels,
        type: 'heatmap',
        colorscale: 'RdBu',
        reversescale: true,
        zmin: -1, zmax: 1,
        colorbar: { title: { text: 'Pearson r', font: { size: 10 } }, thickness: 14 },
        hovertemplate: '%{y} × %{x}: %{z:.3f}<extra></extra>',
    }], baseLayout('Correlation Heatmap (Pearson r)', {
        height: 480,
        annotations: annots,
        xaxis: { tickangle: -30 },
    }), PLOTLY_CONFIG);

    // Click → open scatter
    el.on('plotly_click', (data) => {
        if (!data.points?.length) return;
        const pt = data.points[0];
        const xLabel = pt.x, yLabel = pt.y;
        const xId = numericLayers[labels.indexOf(xLabel)];
        const yId = numericLayers[labels.indexOf(yLabel)];
        if (xId && yId && xId !== yId) {
            // Switch to correlation tab and update dropdowns
            const tab = document.querySelector('[data-page="correlation"]');
            if (tab) tab.click();
            const xSel = document.getElementById('scatter-x');
            const ySel = document.getElementById('scatter-y');
            if (xSel) xSel.value = xId;
            if (ySel) ySel.value = yId;
            renderScatter(xId, yId);
            if (typeof toast === 'function') toast(`Scatter: ${xLabel} vs ${yLabel}`, 'info');
        }
    });
}

function getPearsonFast(gridA, gridB) {
    if (!gridA || !gridB) return null;
    const dataA = gridA.data, dataB = gridB.data;
    const nodataA = gridA.nodata ?? -9999, nodataB = gridB.nodata ?? -9999;
    const sA = gridA.scale || 1, sB = gridB.scale || 1;
    let sumA = 0, sumB = 0, sumAB = 0, sumA2 = 0, sumB2 = 0, count = 0;
    const step = Math.max(1, Math.floor(dataA.length / 5000)); // sample for speed
    for (let i = 0; i < dataA.length; i += step) {
        const rawA = dataA[i], rawB = dataB[i];
        if (rawA === nodataA || rawA <= -9998 || rawA == null || rawB === nodataB || rawB <= -9998 || rawB == null) continue;
        const a = rawA / sA, b = rawB / sB;
        sumA += a; sumB += b; sumAB += a * b; sumA2 += a * a; sumB2 += b * b; count++;
    }
    if (count === 0) return null;
    const num = count * sumAB - sumA * sumB;
    const den = Math.sqrt((count * sumA2 - sumA * sumA) * (count * sumB2 - sumB * sumB));
    return den === 0 ? null : num / den;
}

// ======================================================
// CATEGORICAL CHARTS
// ======================================================

function renderPieFlood(donut = false) {
    const el = document.getElementById('chart-pie-flood');
    if (!el) return;
    const grid = window.gridData?.label;
    if (!grid) { el.innerHTML = chartPlaceholder(); return; }

    let flood = 0, normal = 0;
    for (let i = 0; i < grid.data.length; i++) {
        const v = gridValAt(grid, i);
        if (v === null) continue;
        if (v > 0) flood++; else normal++;
    }

    if (flood + normal === 0) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    Plotly.newPlot(el, [{
        values: [flood, normal],
        labels: ['Ngập Lụt (Flood)', 'Bình Thường (Normal)'],
        type: 'pie',
        hole: donut ? 0.42 : 0,
        marker: { colors: ['#ef4444', '#3b82f6'], line: { color: '#fff', width: 2 } },
        textinfo: 'label+percent',
        hoverinfo: 'label+value+percent',
        pull: [0.04, 0],
    }], baseLayout(`${donut ? 'Donut' : 'Pie'} Chart — Phân Bố Flood Label`, {
        height: 380,
        showlegend: true,
        legend: { orientation: 'v', x: 1.01, y: 0.5 },
        margin: { l: 20, r: 120, t: 48, b: 20 },
    }), PLOTLY_CONFIG);
}

function renderLULCBar() {
    const el = document.getElementById('chart-lulc-bar');
    if (!el) return;
    const grid = window.gridData?.landCover;
    if (!grid) { el.innerHTML = chartPlaceholder(); return; }

    const counts = {};
    for (let i = 0; i < grid.data.length; i++) {
        const v = gridValAt(grid, i);
        if (v === null) continue;
        const cat = Math.round(v);
        counts[cat] = (counts[cat] || 0) + 1;
    }
    const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    if (!sorted.length) {
        el.innerHTML = chartPlaceholder();
        return;
    }
    const labels = sorted.map(([cat]) => `Class ${cat}`);
    const vals = sorted.map(([, cnt]) => cnt);
    const maxVal = Math.max(...vals);

    const barColors = vals.map(v => {
        const ratio = v / maxVal;
        const r = Math.round(59 + (251 - 59) * (1 - ratio));
        const g = Math.round(130 + (146 - 130) * ratio);
        const b = Math.round(246 + (100 - 246) * (1 - ratio));
        return `rgb(${r},${g},${b})`;
    });

    Plotly.newPlot(el, [{
        x: vals, y: labels,
        type: 'bar', orientation: 'h',
        marker: { color: barColors, line: { color: '#e2e8f0', width: 0.5 } },
        text: vals.map(v => v.toLocaleString()),
        textposition: 'outside',
        hovertemplate: '%{y}: %{x:,.0f} pixels<extra></extra>',
    }], baseLayout('LULC Distribution — Số Pixel Theo Class', {
        height: Math.max(320, sorted.length * 36 + 100),
        xaxis: { title: { text: 'Pixel Count', font: { size: 11 } } },
        yaxis: { autorange: 'reversed' },
        margin: { l: 90, r: 80, t: 48, b: 50 },
    }), PLOTLY_CONFIG);
}

function renderTreemap() {
    const el = document.getElementById('chart-treemap');
    if (!el) return;
    const lcGrid = window.gridData?.landCover;
    const labelGrid = window.gridData?.label;
    if (!lcGrid) { el.innerHTML = chartPlaceholder(); return; }

    const counts = {};
    const floodCounts = {};
    for (let i = 0; i < lcGrid.data.length; i++) {
        const v = gridValAt(lcGrid, i);
        if (v === null) continue;
        const cat = `Class ${Math.round(v)}`;
        counts[cat] = (counts[cat] || 0) + 1;
        if (labelGrid) {
            const lv = gridValAt(labelGrid, i);
            if (lv !== null && lv > 0) floodCounts[cat] = (floodCounts[cat] || 0) + 1;
        }
    }

    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    if (total === 0) {
        el.innerHTML = chartPlaceholder();
        return;
    }
    const ids = ['LULC', ...Object.keys(counts)];
    const parents = ['', ...Object.keys(counts).map(() => 'LULC')];
    const values = [0, ...Object.values(counts)];
    const labels = ['Land Cover', ...Object.keys(counts).map(cat => {
        const pct = ((counts[cat] / total) * 100).toFixed(1);
        const fp = floodCounts[cat] ? ((floodCounts[cat] / counts[cat]) * 100).toFixed(0) : '0';
        return `${cat}<br>${pct}%<br>🌊${fp}% flood`;
    })];

    Plotly.newPlot(el, [{
        type: 'treemap',
        ids, parents, values, labels,
        textinfo: 'label+value',
        marker: { colorscale: 'Teal', line: { width: 2, color: '#fff' } },
        hovertemplate: '%{label}<br>Pixels: %{value:,.0f}<extra></extra>',
        branchvalues: 'total',
    }], baseLayout('Treemap — LULC Coverage (% Flood per Class)', {
        height: 440,
        margin: { l: 10, r: 10, t: 48, b: 10 },
    }), PLOTLY_CONFIG);
}

// ======================================================
// ADVANCED CHARTS
// ======================================================

function renderParallelCoords() {
    const el = document.getElementById('chart-parallel');
    if (!el) return;
    const indices = sampleIndices(800);
    const layers = Object.keys(window.gridData || {}).filter(id => id !== 'label');
    const labelGrid = window.gridData?.label;
    if (!layers.length) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    const layersWithData = layers.filter(id =>
        indices.some(i => gridValAt(window.gridData[id], i) !== null)
    );
    if (!layersWithData.length) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    const dimensions = layersWithData.map(id => {
        const vals = indices.map(i => gridValAt(window.gridData[id], i)).filter(v => v !== null);
        const minV = Math.min(...vals), maxV = Math.max(...vals);
        return {
            label: layerLabel(id),
            values: indices.map(i => { const v = gridValAt(window.gridData[id], i); return v ?? minV; }),
            range: [minV, maxV],
        };
    });

    const colorVals = labelGrid
        ? indices.map(i => { const v = gridValAt(labelGrid, i); return v !== null && v > 0 ? 1 : 0; })
        : indices.map(() => 0);

    Plotly.newPlot(el, [{
        type: 'parcoords',
        line: {
            color: colorVals,
            colorscale: [[0, '#3b82f6'], [1, '#ef4444']],
            showscale: true,
            colorbar: { title: { text: 'Flood', font: { size: 10 } }, tickvals: [0, 1], ticktext: ['No', 'Yes'], thickness: 14 },
        },
        dimensions,
    }], baseLayout('Parallel Coordinates — Multi-Layer Correlation (Sample)', {
        height: 460,
        margin: { l: 80, r: 80, t: 60, b: 50 },
    }), PLOTLY_CONFIG);
}

function renderRadarChart() {
    const el = document.getElementById('chart-radar');
    if (!el) return;
    const ids = Object.keys(window.gridData || {});
    if (!ids.length) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    // Compute normalized mean for all layers
    const labels = ids.map(id => layerLabel(id));
    labels.push(labels[0]); // close radar

    const normalVals = [], floodVals = [];
    const labelGrid = window.gridData?.label;

    ids.forEach(id => {
        const grid = window.gridData[id];
        const allV = [], fV = [], nV = [];
        const step = Math.max(1, Math.floor(grid.data.length / 5000));
        for (let i = 0; i < grid.data.length; i += step) {
            const v = gridValAt(grid, i);
            if (v === null) continue;
            allV.push(v);
            if (labelGrid) {
                const lv = gridValAt(labelGrid, i);
                if (lv !== null && lv > 0) fV.push(v); else nV.push(v);
            }
        }
        const scaleMin = Math.min(...allV), scaleMax = Math.max(...allV);
        const norm = v => (scaleMax - scaleMin) > 0 ? (v - scaleMin) / (scaleMax - scaleMin) : 0;
        normalVals.push(nV.length ? norm(mean(nV)) : norm(mean(allV)));
        floodVals.push(fV.length ? norm(mean(fV)) : norm(mean(allV)));
    });
    normalVals.push(normalVals[0]);
    floodVals.push(floodVals[0]);

    if (!ids.some(id => {
        const grid = window.gridData[id];
        for (let i = 0; i < grid.data.length; i += Math.max(1, Math.floor(grid.data.length / 100))) {
            if (gridValAt(grid, i) !== null) return true;
        }
        return false;
    })) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    Plotly.newPlot(el, [
        {
            type: 'scatterpolar', r: normalVals, theta: labels,
            fill: 'toself', name: 'Normal Pixels',
            line: { color: '#3b82f6', width: 2 },
            marker: { color: '#3b82f6', size: 5 },
            fillcolor: 'rgba(59,130,246,0.15)',
        },
        {
            type: 'scatterpolar', r: floodVals, theta: labels,
            fill: 'toself', name: 'Flood Pixels',
            line: { color: '#ef4444', width: 2 },
            marker: { color: '#ef4444', size: 5 },
            fillcolor: 'rgba(239,68,68,0.15)',
        }
    ], baseLayout('Radar Chart — Normalized Layer Means (Flood vs Normal)', {
        polar: {
            radialaxis: { visible: true, range: [0, 1], tickfont: { size: 9 } },
            angularaxis: { tickfont: { size: 10 } },
        },
        showlegend: true,
        legend: { orientation: 'h', y: -0.12 },
        height: 460,
        margin: { l: 60, r: 60, t: 60, b: 80 },
    }), PLOTLY_CONFIG);
}

function renderQuantileComparison() {
    const el = document.getElementById('chart-quantile');
    if (!el) return;
    const ids = Object.keys(window.gridData || {});
    if (!ids.length) return;

    const traces = [];
    const quantiles = [0, 0.1, 0.25, 0.5, 0.75, 0.9, 1];
    const qLabels = ['Min', 'P10', 'Q1', 'Median', 'Q3', 'P90', 'Max'];

    ids.forEach(id => {
        const vals = extractValues(id).sort((a, b) => a - b);
        if (!vals.length) return;
        const n = vals.length;
        const qs = quantiles.map(q => vals[Math.floor(q * (n - 1))]);
        traces.push({
            x: qLabels, y: qs,
            type: 'scatter', mode: 'lines+markers',
            name: layerLabel(id),
            line: { color: layerColor(id), width: 2 },
            marker: { color: layerColor(id), size: 7 },
            hovertemplate: `${layerLabel(id)} %{x}: %{y:.3f}<extra></extra>`,
        });
    });

    if (!traces.length) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    Plotly.newPlot(el, traces, baseLayout('Quantile Profile — Tất Cả Layers (Normalized Trend)', {
        height: 400,
        xaxis: { title: { text: 'Quantile', font: { size: 11 } } },
        yaxis: { title: { text: 'Raw Value', font: { size: 11 } } },
        showlegend: true,
        legend: { orientation: 'h', y: -0.2 },
    }), PLOTLY_CONFIG);
}

function renderFloodProfileBar() {
    const el = document.getElementById('chart-flood-profile');
    if (!el) return;
    const ids = Object.keys(window.gridData || {}).filter(id => id !== 'label');
    const labelGrid = window.gridData?.label;
    if (!ids.length || !labelGrid) {
        el.innerHTML = chartPlaceholder();
        return;
    }

    const step = Math.max(1, Math.floor(labelGrid.data.length / 5000));
    const floodMeans = {}, normalMeans = {};

    ids.forEach(id => {
        const grid = window.gridData[id];
        let fs = 0, fc = 0, ns = 0, nc = 0;
        for (let i = 0; i < grid.data.length; i += step) {
            const v = gridValAt(grid, i), lv = gridValAt(labelGrid, i);
            if (v === null || lv === null) continue;
            if (lv > 0) { fs += v; fc++; } else { ns += v; nc++; }
        }
        floodMeans[id] = fc > 0 ? (fs / fc) : 0;
        normalMeans[id] = nc > 0 ? (ns / nc) : 0;
    });

    // Normalize to [0,1] range across each layer for comparison
    const labels = ids.map(id => layerLabel(id));

    Plotly.newPlot(el, [
        {
            x: labels,
            y: ids.map(id => {
                const range = Math.max(Math.abs(floodMeans[id]), Math.abs(normalMeans[id]), 0.001);
                return floodMeans[id] / range;
            }),
            name: 'Flood Mean (norm.)', type: 'bar',
            marker: { color: '#ef4444', opacity: 0.82 },
        },
        {
            x: labels,
            y: ids.map(id => {
                const range = Math.max(Math.abs(floodMeans[id]), Math.abs(normalMeans[id]), 0.001);
                return normalMeans[id] / range;
            }),
            name: 'Normal Mean (norm.)', type: 'bar',
            marker: { color: '#3b82f6', opacity: 0.82 },
        }
    ], baseLayout('Flood vs Normal — Normalized Mean Per Layer', {
        height: 400,
        barmode: 'group',
        xaxis: { tickangle: -20 },
        yaxis: { title: { text: 'Normalized Mean', font: { size: 11 } } },
        showlegend: true,
        legend: { orientation: 'h', y: -0.2 },
    }), PLOTLY_CONFIG);
}

// ======================================================
// INIT & EVENT WIRING
// ======================================================

function initDistributionControls() {
    const layerSelect = document.getElementById('hist-layer-select');
    const binSlider = document.getElementById('hist-bins');
    const binLabel = document.getElementById('hist-bins-label');
    const violinSelect = document.getElementById('violin-layer-select');

    const firstLayer = window.LAYERS?.[0]?.id || Object.keys(window.gridData||{})[0];
    if (!firstLayer) return;

    // Populate selects
    [layerSelect, violinSelect].forEach(sel => {
        if (!sel) return;
        sel.innerHTML = '';
        (window.LAYERS || []).forEach(l => {
            const opt = document.createElement('option');
            opt.value = l.id; opt.textContent = l.label;
            sel.appendChild(opt);
        });
        sel.value = firstLayer;
    });

    if (layerSelect) {
        layerSelect.addEventListener('change', () => renderHistogram(layerSelect.value, parseInt(binSlider?.value || 40)));
    }
    if (binSlider) {
        binSlider.addEventListener('input', () => {
            if (binLabel) binLabel.textContent = binSlider.value;
            renderHistogram(layerSelect?.value || firstLayer, parseInt(binSlider.value));
        });
    }
    if (violinSelect) {
        violinSelect.addEventListener('change', () => renderViolinPlot(violinSelect.value));
    }
}

function initCorrelationControls() {
    const layers = window.LAYERS || [];
    const xSel = document.getElementById('scatter-x');
    const ySel = document.getElementById('scatter-y');
    if (!xSel || !ySel) return;

    xSel.innerHTML = ySel.innerHTML = '';
    layers.forEach(l => {
        const makeOpt = () => { const o = document.createElement('option'); o.value = l.id; o.textContent = l.label; return o; };
        xSel.appendChild(makeOpt());
        ySel.appendChild(makeOpt());
    });
    if (layers.length > 1) ySel.value = layers[1].id;

    const refresh = () => renderScatter(xSel.value, ySel.value);
    xSel.addEventListener('change', refresh);
    ySel.addEventListener('change', refresh);
}

function initPieToggle() {
    const btn = document.getElementById('btn-toggle-donut');
    let isDonut = false;
    if (!btn) return;
    btn.addEventListener('click', () => {
        isDonut = !isDonut;
        btn.textContent = isDonut ? 'Chuyển sang Pie' : 'Chuyển sang Donut';
        renderPieFlood(isDonut);
    });
}

function renderAllCharts() {
    const firstId = window.LAYERS?.[0]?.id || Object.keys(window.gridData || {})[0];
    if (!firstId) return;

    // Init controls
    initDistributionControls();
    initCorrelationControls();
    initPieToggle();

    // Distribution
    renderHistogram(firstId, 40);
    renderBoxPlot();
    renderViolinPlot(firstId);
    renderQuantileComparison();

    // Correlation
    const layers = Object.keys(window.gridData || {});
    renderScatter(layers[0] || 'dem', layers[1] || 'rain');
    renderBubbleChart();
    renderCorrelationHeatmap();

    // Categorical
    renderPieFlood(false);
    renderLULCBar();
    renderTreemap();

    // Advanced
    renderParallelCoords();
    renderRadarChart();
    renderFloodProfileBar();

    EDACharts.initialized = true;
    console.log('[EDA Charts] All charts rendered');
}

// ── Main Entry ──
document.addEventListener('edaDataLoaded', () => {
    console.log('[EDA Charts] Data loaded, rendering charts...');
    renderAllCharts();
});
