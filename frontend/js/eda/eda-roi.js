/**
 * eda-roi.js — ROI (Region of Interest) Selection & Analysis
 * Click-drag to select a region on the DEM heatmap, then analyze statistics for that region.
 * Includes ROI presets: Low Elevation, Riverside, Flood Zone.
 */
'use strict';

// ROI State
window.EDA_ROI = { active: false, r0: 0, c0: 0, r1: 0, c1: 0 };

function renderROIPage() {
    const G = window.EDA.gridData;
    const demG = G.dem, lblG = G.label;
    if (!demG) return;

    const rows = demG.size.r, cols = demG.size.c;
    const b = demG.bounds;
    const ds = 5;
    const dr = Math.ceil(rows / ds), dc = Math.ceil(cols / ds);

    // DEM overview for selection
    const { z: zDem } = dsGrid(demG, ds);
    Plotly.newPlot('roi-map', [{
        z: zDem, type: 'heatmap',
        x0: b.w, dx: (b.e - b.w) / dc,
        y0:b.n, dy:-(b.n-b.s) / dr,
        colorscale: 'Earth', zsmooth: 'best',
        colorbar: { thickness: 12, tickfont: { size: 9, color: '#94a3b8' } },
        hovertemplate: 'Lat:%{y:.3f}<br>Lng:%{x:.3f}<br>DEM:%{z:.1f}m<extra></extra>'
    }], darkLayout('Click & Drag to select ROI (DEM Base Map)', {
        height: 450,
        xaxis: { title: { text: 'Longitude', font: { size: 10 } } },
        yaxis: { title: { text: 'Latitude', font: { size: 10 } }, scaleanchor: 'x' },
        dragmode: 'select'
    }), { ...PLOTLY_CFG, modeBarButtonsToRemove: [] });

    // Listen for selection
    const roiMap = document.getElementById('roi-map');
    if (roiMap) {
        roiMap.on('plotly_selected', function(eventData) {
            if (!eventData || !eventData.range) return;
            const xRange = eventData.range.x;
            const yRange = eventData.range.y;
            applyROI(xRange[0], xRange[1], yRange[0], yRange[1]);
        });
    }

    // Preset buttons
    document.getElementById('btn-roi-low')?.addEventListener('click', () => {
        applyPresetROI('low_elevation');
    });
    document.getElementById('btn-roi-river')?.addEventListener('click', () => {
        applyPresetROI('riverside');
    });
    document.getElementById('btn-roi-flood')?.addEventListener('click', () => {
        applyPresetROI('flood_zone');
    });
    document.getElementById('btn-roi-reset')?.addEventListener('click', () => {
        window.EDA_ROI.active = false;
        document.getElementById('roi-stats-panel').innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-muted)"><span class="material-icons" style="font-size:48px;opacity:0.3">touch_app</span><p style="margin-top:10px">Chọn vùng trên bản đồ hoặc dùng Preset</p></div>';
        document.getElementById('roi-compare').innerHTML = '';
        document.getElementById('roi-distribution').innerHTML = '';
        toast('ROI đã xóa', 'info');
    });
}

function applyPresetROI(preset) {
    const G = window.EDA.gridData;
    const demG = G.dem, flowG = G.flow, lblG = G.label;
    if (!demG) return;

    const rows = demG.size.r, cols = demG.size.c;
    const step = Math.max(1, Math.floor(demG.data.length / 20000));
    const mask = new Uint8Array(demG.data.length);
    let count = 0;

    if (preset === 'low_elevation' && demG) {
        for (let i = 0; i < demG.data.length; i++) {
            const v = gridVal(demG, i);
            if (v !== null && v < 10) { mask[i] = 1; count++; }
        }
        toast(`ROI: Vùng thấp (DEM < 10m) — ${count.toLocaleString()} pixels`, 'success');
    } else if (preset === 'riverside' && flowG) {
        // Top 5% flow accumulation
        const vals = extractValues('flow', 5000).sort((a, b) => b - a);
        const threshold = vals[Math.floor(vals.length * 0.05)] || 0;
        for (let i = 0; i < flowG.data.length; i++) {
            const v = gridVal(flowG, i);
            if (v !== null && v >= threshold) { mask[i] = 1; count++; }
        }
        toast(`ROI: Ven sông (Flow > ${threshold.toFixed(0)}) — ${count.toLocaleString()} pixels`, 'success');
    } else if (preset === 'flood_zone' && lblG) {
        for (let i = 0; i < lblG.data.length; i++) {
            const v = gridVal(lblG, i);
            if (v !== null && v > 0) { mask[i] = 1; count++; }
        }
        toast(`ROI: Vùng ngập (Flood > 0) — ${count.toLocaleString()} pixels`, 'success');
    }

    if (count === 0) { toast('Không tìm thấy pixel phù hợp cho preset này', 'error'); return; }

    analyzeROIByMask(mask, count);
}

function applyROI(lng0, lng1, lat0, lat1) {
    const G = window.EDA.gridData;
    const demG = G.dem;
    if (!demG) return;

    const b = demG.bounds, rows = demG.size.r, cols = demG.size.c;
    const mask = new Uint8Array(demG.data.length);
    let count = 0;

    for (let r = 0; r < rows; r++) {
        const lat = b.n - (r / rows) * (b.n - b.s);
        if (lat < lat0 || lat > lat1) continue;
        for (let c = 0; c < cols; c++) {
            const lng = b.w + (c / cols) * (b.e - b.w);
            if (lng < lng0 || lng > lng1) continue;
            mask[r * cols + c] = 1;
            count++;
        }
    }

    toast(`ROI selected: ${count.toLocaleString()} pixels (${lat0.toFixed(3)}–${lat1.toFixed(3)}, ${lng0.toFixed(3)}–${lng1.toFixed(3)})`, 'success');
    analyzeROIByMask(mask, count);
}

function analyzeROIByMask(mask, total) {
    const G = window.EDA.gridData;
    const statsPanel = document.getElementById('roi-stats-panel');
    const compareEl = document.getElementById('roi-compare');
    const distEl = document.getElementById('roi-distribution');

    // Calculate stats for each layer within ROI
    const roiStats = [];
    const globalStats = [];
    const numLayers = window.LAYERS.filter(l => !l.isCat);

    numLayers.forEach(layer => {
        const grid = G[layer.id];
        if (!grid) return;
        const d = grid.data, nd = grid.nodata ?? -9999, sc = grid.scale || 1;

        let rSum = 0, rCount = 0, rMin = Infinity, rMax = -Infinity;
        let gSum = 0, gCount = 0;

        for (let i = 0; i < d.length; i++) {
            const raw = d[i];
            if (raw === nd || raw <= -9998 || raw == null) continue;
            const v = raw / sc;
            gSum += v; gCount++;
            if (mask[i]) {
                rSum += v; rCount++;
                if (v < rMin) rMin = v;
                if (v > rMax) rMax = v;
            }
        }

        const rMean = rCount > 0 ? rSum / rCount : 0;
        const gMean = gCount > 0 ? gSum / gCount : 0;

        roiStats.push({ id: layer.id, label: layer.label, color: layer.color, unit: layer.unit, mean: rMean, min: rMin === Infinity ? 0 : rMin, max: rMax === -Infinity ? 0 : rMax, count: rCount });
        globalStats.push({ id: layer.id, mean: gMean });
    });

    // Flood analysis in ROI
    const lblG = G.label;
    let floodInRoi = 0, totalInRoi = 0;
    if (lblG) {
        for (let i = 0; i < lblG.data.length; i++) {
            if (!mask[i]) continue;
            const v = gridVal(lblG, i);
            if (v === null) continue;
            totalInRoi++;
            if (v > 0) floodInRoi++;
        }
    }
    const floodPct = totalInRoi > 0 ? (floodInRoi / totalInRoi * 100) : 0;

    // Render stats panel
    let html = `
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px;margin-bottom:16px">
        <div style="background:rgba(99,102,241,0.12);border:1px solid rgba(99,102,241,0.3);border-radius:10px;padding:14px;text-align:center">
            <div style="font-size:10px;color:var(--accent);text-transform:uppercase;font-weight:700">ROI Pixels</div>
            <div style="font-size:24px;font-weight:800;color:#fff">${total.toLocaleString()}</div>
        </div>
        <div style="background:rgba(239,68,68,0.12);border:1px solid rgba(239,68,68,0.3);border-radius:10px;padding:14px;text-align:center">
            <div style="font-size:10px;color:#f87171;text-transform:uppercase;font-weight:700">Flood in ROI</div>
            <div style="font-size:24px;font-weight:800;color:#ef4444">${floodPct.toFixed(1)}%</div>
        </div>`;

    roiStats.forEach(s => {
        html += `
        <div style="background:rgba(30,41,59,0.6);border:1px solid var(--glass-border);border-radius:10px;padding:14px">
            <div style="font-size:10px;color:${s.color};text-transform:uppercase;font-weight:700">${s.label}</div>
            <div style="font-size:16px;font-weight:700;color:var(--text)">${s.mean.toFixed(3)} <span style="font-size:10px;color:var(--text-muted)">${s.unit}</span></div>
            <div style="font-size:10px;color:var(--text-muted)">${s.min.toFixed(2)} — ${s.max.toFixed(2)}</div>
        </div>`;
    });
    html += '</div>';
    statsPanel.innerHTML = html;

    // Compare ROI vs Global bar chart
    const labels = roiStats.map(s => s.label);
    const roiMeans = roiStats.map(s => s.mean);
    const gMeans = roiStats.map((s, i) => globalStats[i]?.mean || 0);

    // Normalize for comparison
    const maxVals = roiMeans.map((r, i) => Math.max(Math.abs(r), Math.abs(gMeans[i]), 0.001));
    Plotly.newPlot(compareEl, [
        { x: labels, y: roiMeans.map((v, i) => v / maxVals[i]), name: 'ROI Mean', type: 'bar', marker: { color: '#6366f1', opacity: 0.85 } },
        { x: labels, y: gMeans.map((v, i) => v / maxVals[i]), name: 'Global Mean', type: 'bar', marker: { color: '#64748b', opacity: 0.6 } }
    ], darkLayout('ROI vs Global — Normalized Mean', { height: 350, barmode: 'group', legend: { orientation: 'h', y: -0.2, font: { color: '#94a3b8' } } }), PLOTLY_CFG);

    // Distribution within ROI
    const distTraces = [];
    numLayers.slice(0, 4).forEach(layer => {
        const grid = G[layer.id];
        if (!grid) return;
        const vals = [];
        const step = Math.max(1, Math.floor(grid.data.length / 10000));
        for (let i = 0; i < grid.data.length; i += step) {
            if (!mask[i]) continue;
            const v = gridVal(grid, i);
            if (v !== null) vals.push(v);
        }
        if (vals.length > 0) {
            distTraces.push({ x: vals, type: 'histogram', nbinsx: 40, opacity: 0.6, marker: { color: layer.color }, name: layer.label });
        }
    });

    if (distTraces.length) {
        Plotly.newPlot(distEl, distTraces, darkLayout('Distribution trong ROI (top 4 layers)', { height: 350, barmode: 'overlay', legend: { orientation: 'h', y: -0.2, font: { color: '#94a3b8', size: 10 } } }), PLOTLY_CFG);
    }
}

// Helper: dsGrid is defined in eda-spatial.js, check availability
if (typeof dsGrid === 'undefined') {
    window.dsGrid = function(grid, ds) {
        const rows = grid.size.r, cols = grid.size.c;
        const dr = Math.ceil(rows / ds), dc = Math.ceil(cols / ds);
        const z = [];
        for (let r = 0; r < dr; r++) { const row = []; for (let c = 0; c < dc; c++) row.push(gridVal(grid, Math.min(r * ds, rows - 1) * cols + Math.min(c * ds, cols - 1))); z.push(row); }
        return { z, dr, dc };
    };
}

document.addEventListener('edaDataReady', renderROIPage);
