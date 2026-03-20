/**
 * eda-correlation.js — Tương Quan: Original 5 + 4 new (Hexbin, 2D Density, SPLOM, Spearman)
 */
'use strict';

const CORR_NO_DATA = 'nodata';

function pearsonFast(gA, gB) {
    if (!gA || !gB) return null;
    const dA = gA.data, dB = gB.data, ndA = gA.nodata ?? -9999, ndB = gB.nodata ?? -9999;
    const sA = gA.scale || 1, sB = gB.scale || 1;
    let sa = 0, sb = 0, sab = 0, sa2 = 0, sb2 = 0, c = 0;
    const step = Math.max(1, Math.floor(dA.length / 5000));
    for (let i = 0; i < dA.length; i += step) {
        const rA = dA[i], rB = dB[i];
        if (rA === ndA || rA <= -9998 || rB === ndB || rB <= -9998) continue;
        const a = rA / sA, b = rB / sB;
        sa += a; sb += b; sab += a * b; sa2 += a * a; sb2 += b * b; c++;
    }
    if (!c) return null;
    const num = c * sab - sa * sb, den = Math.sqrt((c * sa2 - sa * sa) * (c * sb2 - sb * sb));
    return den === 0 ? null : num / den;
}

function spearmanFast(gA, gB) {
    if (!gA || !gB) return null;
    const step = Math.max(1, Math.floor(gA.data.length / 3000));
    const pairs = [];
    for (let i = 0; i < gA.data.length; i += step) {
        const a = gridVal(gA, i), b = gridVal(gB, i);
        if (a !== null && b !== null) pairs.push({ a, b, i: pairs.length });
    }
    if (pairs.length < 10) return null;
    const rankA = [...pairs].sort((x, y) => x.a - y.a).map((p, i) => ({ ...p, rA: i }));
    const mapA = {}; rankA.forEach(p => mapA[p.i] = p.rA);
    const rankB = [...pairs].sort((x, y) => x.b - y.b).map((p, i) => ({ ...p, rB: i }));
    const mapB = {}; rankB.forEach(p => mapB[p.i] = p.rB);
    let sumD2 = 0;
    for (let i = 0; i < pairs.length; i++) sumD2 += (mapA[i] - mapB[i]) ** 2;
    const n = pairs.length;
    return 1 - (6 * sumD2) / (n * (n * n - 1));
}

function renderScatterXY(xId, yId) {
    const el = document.getElementById('chart-scatter'); if (!el) return;
    const G = window.EDA.gridData, indices = sampleIdx(1500);
    const xG = G[xId], yG = G[yId], lG = G.label;
    if (!xG || !yG) return;
    const x = [], y = [], colors = [];
    for (const i of indices) { const xv = gridVal(xG, i), yv = gridVal(yG, i); if (xv === null || yv === null) continue; x.push(xv); y.push(yv); colors.push(lG && gridVal(lG, i) > 0 ? '#ef4444' : '#3b82f6'); }
    if (!x.length) { el.innerHTML = `<p class="text-slate-400 text-center p-8">${CORR_NO_DATA}</p>`; return; }
    Plotly.newPlot(el, [{ x, y, mode: 'markers', type: 'scatter', marker: { color: colors, size: 4, opacity: 0.6 } }], darkLayout(`${layerLabel(xId)} vs ${layerLabel(yId)}`, { height: 400, xaxis: { title: { text: layerLabel(xId), font: { size: 10 } } }, yaxis: { title: { text: layerLabel(yId), font: { size: 10 } } } }), PLOTLY_CFG);
}

function renderCorrelationPage() {
    const G = window.EDA.gridData;
    const numL = window.LAYERS.filter(l => !l.isCat).map(l => l.id);
    const allIds = Object.keys(G);

    // Pearson Heatmap
    const corrEl = document.getElementById('chart-corr-heatmap');
    if (corrEl && numL.length) {
        const n = numL.length, labels = numL.map(id => layerLabel(id));
        const zM = [], annots = [];
        for (let i = 0; i < n; i++) { const row = []; for (let j = 0; j < n; j++) { const v = i === j ? 1 : pearsonFast(G[numL[i]], G[numL[j]]); row.push(v); const isNum = typeof v === 'number' && !Number.isNaN(v); annots.push({ x: labels[j], y: labels[i], text: isNum ? v.toFixed(2) : CORR_NO_DATA, showarrow: false, font: { size: 10, color: isNum && Math.abs(v) > 0.5 ? '#fff' : '#94a3b8' } }); } zM.push(row); }
        Plotly.newPlot(corrEl, [{ z: zM, x: labels, y: labels, type: 'heatmap', colorscale: 'RdBu', reversescale: true, zmin: -1, zmax: 1, colorbar: { thickness: 12, tickfont: { color: '#94a3b8' } } }], darkLayout('Pearson Correlation', { height: 420, annotations: annots, xaxis: { tickangle: -30 } }), PLOTLY_CFG);
    }

    // ── NEW: Spearman ──
    const spEl = document.getElementById('chart-spearman');
    if (spEl && numL.length) {
        const n = numL.length, labels = numL.map(id => layerLabel(id));
        const zM = [], annots = [];
        for (let i = 0; i < n; i++) { const row = []; for (let j = 0; j < n; j++) { const v = i === j ? 1 : spearmanFast(G[numL[i]], G[numL[j]]); row.push(v); const isNum = typeof v === 'number' && !Number.isNaN(v); annots.push({ x: labels[j], y: labels[i], text: isNum ? v.toFixed(2) : CORR_NO_DATA, showarrow: false, font: { size: 10, color: isNum && Math.abs(v) > 0.5 ? '#fff' : '#94a3b8' } }); } zM.push(row); }
        Plotly.newPlot(spEl, [{ z: zM, x: labels, y: labels, type: 'heatmap', colorscale: 'RdBu', reversescale: true, zmin: -1, zmax: 1, colorbar: { thickness: 12, tickfont: { color: '#94a3b8' } } }], darkLayout('Spearman Rank Correlation', { height: 420, annotations: annots, xaxis: { tickangle: -30 } }), PLOTLY_CFG);
    }

    // Scatter selects
    ['scatter-x', 'scatter-y', 'hexbin-x', 'hexbin-y', 'density2d-x', 'density2d-y'].forEach(selId => {
        const sel = document.getElementById(selId); if (!sel) return;
        sel.innerHTML = '';
        window.LAYERS.forEach(l => { const o = document.createElement('option'); o.value = l.id; o.textContent = l.label; sel.appendChild(o); });
    });
    const xS = document.getElementById('scatter-x'), yS = document.getElementById('scatter-y');
    if (xS && yS && window.LAYERS.length > 1) { yS.value = window.LAYERS[1].id; xS.onchange = yS.onchange = () => renderScatterXY(xS.value, yS.value); renderScatterXY(xS.value, yS.value); }

    // ── NEW: Hexbin ──
    const hxS = document.getElementById('hexbin-x'), hyS = document.getElementById('hexbin-y');
    function renderHexbin(xId, yId) {
        const el = document.getElementById('chart-hexbin'); if (!el) return;
        const indices = sampleIdx(3000), xG = G[xId], yG = G[yId]; if (!xG || !yG) return;
        const x = [], y = [];
        for (const i of indices) { const xv = gridVal(xG, i), yv = gridVal(yG, i); if (xv !== null && yv !== null) { x.push(xv); y.push(yv); } }
        if (!x.length) { el.innerHTML = `<p class="text-slate-400 text-center p-8">${CORR_NO_DATA}</p>`; return; }
        Plotly.newPlot(el, [{ x, y, type: 'histogram2d', nbinsx: 30, nbinsy: 30, colorscale: 'Viridis', colorbar: { thickness: 12, tickfont: { size: 8, color: '#94a3b8' } } }], darkLayout(`Hexbin — ${layerLabel(xId)} vs ${layerLabel(yId)}`, { height: 400, xaxis: { title: { text: layerLabel(xId), font: { size: 10 } } }, yaxis: { title: { text: layerLabel(yId), font: { size: 10 } } } }), PLOTLY_CFG);
    }
    if (hxS && hyS) { if (window.LAYERS.length > 1) hyS.value = window.LAYERS[1].id; hxS.onchange = hyS.onchange = () => renderHexbin(hxS.value, hyS.value); renderHexbin(hxS.value, hyS.value); }

    // ── NEW: 2D Density Contour ──
    const dxS = document.getElementById('density2d-x'), dyS = document.getElementById('density2d-y');
    function renderDensity2D(xId, yId) {
        const el = document.getElementById('chart-density2d'); if (!el) return;
        const indices = sampleIdx(2000), xG = G[xId], yG = G[yId]; if (!xG || !yG) return;
        const x = [], y = [];
        for (const i of indices) { const xv = gridVal(xG, i), yv = gridVal(yG, i); if (xv !== null && yv !== null) { x.push(xv); y.push(yv); } }
        if (!x.length) { el.innerHTML = `<p class="text-slate-400 text-center p-8">${CORR_NO_DATA}</p>`; return; }
        Plotly.newPlot(el, [
            { x, y, type: 'histogram2dcontour', colorscale: 'Hot', reversescale: true, showscale: true, colorbar: { thickness: 10, tickfont: { size: 8, color: '#94a3b8' } }, contours: { coloring: 'heatmap' } },
            { x, y, type: 'scatter', mode: 'markers', marker: { color: 'rgba(255,255,255,0.15)', size: 2 }, showlegend: false }
        ], darkLayout(`2D Density — ${layerLabel(xId)} vs ${layerLabel(yId)}`, { height: 400, xaxis: { title: { text: layerLabel(xId), font: { size: 10 } } }, yaxis: { title: { text: layerLabel(yId), font: { size: 10 } } } }), PLOTLY_CFG);
    }
    if (dxS && dyS) { if (window.LAYERS.length > 1) dyS.value = window.LAYERS[1].id; dxS.onchange = dyS.onchange = () => renderDensity2D(dxS.value, dyS.value); renderDensity2D(dxS.value, dyS.value); }

    // ── NEW: SPLOM ──
    const splomEl = document.getElementById('chart-splom');
    if (splomEl) {
        const layersForSplom = numL.slice(0, 6);
        const indices = sampleIdx(500);
        const dims = layersForSplom.map(id => ({ label: layerLabel(id), values: indices.map(i => gridVal(G[id], i) || 0) }));
        const colorVals = G.label ? indices.map(i => gridVal(G.label, i) > 0 ? 1 : 0) : indices.map(() => 0);
        Plotly.newPlot(splomEl, [{ type: 'splom', dimensions: dims, marker: { color: colorVals, colorscale: [[0, '#3b82f6'], [1, '#ef4444']], size: 3, opacity: 0.5, showscale: false } }], darkLayout('Scatter Matrix (SPLOM)', { height: 650, margin: { l: 60, r: 30, t: 44, b: 60 } }), PLOTLY_CFG);
    }

    // Bubble
    const bubEl = document.getElementById('chart-bubble');
    if (bubEl) {
        if (!G.dem || !G.rain) {
            bubEl.innerHTML = `<p class="text-slate-400 text-center p-8">${CORR_NO_DATA}</p>`;
        } else {
            const idx = sampleIdx(800), x = [], y = [], sz = [], cl = [];
            for (const i of idx) { const d = gridVal(G.dem, i), r = gridVal(G.rain, i); if (d === null || r === null) continue; const f = G.flow ? (gridVal(G.flow, i) || 0) : 1; const l = G.label ? (gridVal(G.label, i) || 0) : 0; x.push(d); y.push(r); sz.push(Math.max(4, Math.min(28, Math.log1p(Math.abs(f)) * 2))); cl.push(l > 0 ? '#ef4444' : '#3b82f6'); }
            if (!x.length) bubEl.innerHTML = `<p class="text-slate-400 text-center p-8">${CORR_NO_DATA}</p>`;
            else Plotly.newPlot(bubEl, [{ x, y, mode: 'markers', type: 'scatter', marker: { color: cl, size: sz, opacity: 0.7 } }], darkLayout('Bubble — DEM vs Rain', { height: 420 }), PLOTLY_CFG);
        }
    }

    // Parallel Coordinates
    const parEl = document.getElementById('chart-parallel');
    if (parEl) {
        const idx = sampleIdx(800), layers = allIds.filter(id => id !== 'label');
        const dims = layers.map(id => { const vals = idx.map(i => gridVal(G[id], i)).filter(v => v !== null); const mn = Math.min(...vals), mx = Math.max(...vals); return { label: layerLabel(id), values: idx.map(i => gridVal(G[id], i) ?? mn), range: [mn, mx] }; });
        const cv = G.label ? idx.map(i => gridVal(G.label, i) > 0 ? 1 : 0) : idx.map(() => 0);
        Plotly.newPlot(parEl, [{ type: 'parcoords', line: { color: cv, colorscale: [[0, '#3b82f6'], [1, '#ef4444']] }, dimensions: dims, labelfont: { color: '#94a3b8' }, tickfont: { color: '#64748b' } }], darkLayout('Parallel Coordinates', { height: 460, margin: { l: 70, r: 70, t: 44, b: 50 } }), PLOTLY_CFG);
    }

    // Radar
    const radEl = document.getElementById('chart-radar');
    if (radEl && allIds.length) {
        const mean = arr => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
        const labels = allIds.map(id => layerLabel(id)); labels.push(labels[0]);
        const nV = [], fV = [];
        allIds.forEach(id => { const g = G[id], aV = [], fArr = [], nArr = []; const step = Math.max(1, Math.floor(g.data.length / 5000)); for (let i = 0; i < g.data.length; i += step) { const v = gridVal(g, i); if (v === null) continue; aV.push(v); if (G.label) { gridVal(G.label, i) > 0 ? fArr.push(v) : nArr.push(v); } } const mn = Math.min(...aV), r = (Math.max(...aV) - mn) || 1; const norm = v => (v - mn) / r; nV.push(nArr.length ? norm(mean(nArr)) : norm(mean(aV))); fV.push(fArr.length ? norm(mean(fArr)) : norm(mean(aV))); });
        nV.push(nV[0]); fV.push(fV[0]);
        Plotly.newPlot(radEl, [
            { type: 'scatterpolar', r: nV, theta: labels, fill: 'toself', name: 'Normal', line: { color: '#3b82f6' }, fillcolor: 'rgba(59,130,246,0.15)' },
            { type: 'scatterpolar', r: fV, theta: labels, fill: 'toself', name: 'Flood', line: { color: '#ef4444' }, fillcolor: 'rgba(239,68,68,0.15)' }
        ], darkLayout('Radar — Flood vs Normal', { polar: { radialaxis: { visible: true, range: [0, 1], tickfont: { size: 8, color: '#64748b' } }, angularaxis: { tickfont: { size: 10, color: '#94a3b8' } }, bgcolor: 'rgba(0,0,0,0)' }, showlegend: true, legend: { orientation: 'h', y: -0.15, font: { color: '#94a3b8' } }, height: 460 }), PLOTLY_CFG);
    }
}

document.addEventListener('edaDataReady', renderCorrelationPage);
