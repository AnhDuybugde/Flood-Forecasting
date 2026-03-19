/**
 * eda-distribution.js — Phân Phối: Original 4 + 4 new (KDE, CDF, Ridgeline, Strip)
 */
'use strict';

function renderDistributionPage() {
    const firstId = window.LAYERS[0]?.id || Object.keys(window.EDA.gridData)[0];
    if (!firstId) return;

    ['hist-layer-select','violin-layer-select','kde-layer-select','cdf-layer-select'].forEach(selId => {
        const sel = document.getElementById(selId); if (!sel) return;
        sel.innerHTML = '';
        window.LAYERS.forEach(l => { const o = document.createElement('option'); o.value = l.id; o.textContent = l.label; sel.appendChild(o); });
        sel.value = firstId;
    });

    function renderHistogram(lid, bins) {
        const el = document.getElementById('chart-histogram'); if (!el) return;
        const xs = extractValues(lid, 0);
        if (!xs.length) { el.innerHTML = '<p class="text-slate-400 text-center p-8">nodata</p>'; return; }
        Plotly.newPlot(el, [{ x: xs, type:'histogram', nbinsx:bins, marker:{color:layerColor(lid),opacity:0.85} }], darkLayout(`Histogram — ${layerLabel(lid)}`, { height:380, bargap:0.08, xaxis:{title:{text:layerLabel(lid),font:{size:10}}}, yaxis:{title:{text:'Count',font:{size:10}}} }), PLOTLY_CFG);
    }
    function renderViolin(lid) {
        const el = document.getElementById('chart-violin'); if (!el) return;
        const ys = extractValues(lid, 3000);
        if (!ys.length) { el.innerHTML = '<p class="text-slate-400 text-center p-8">nodata</p>'; return; }
        Plotly.newPlot(el, [{ y: ys, type:'violin', name:layerLabel(lid), box:{visible:true}, meanline:{visible:true,color:'#fbbf24',width:2}, fillcolor:layerColor(lid), opacity:0.7, line:{color:layerColor(lid)} }], darkLayout(`Violin — ${layerLabel(lid)}`, { height:380, showlegend:false }), PLOTLY_CFG);
    }
    function renderKDE(lid) {
        const el = document.getElementById('chart-kde'); if (!el) return;
        const vals = extractValues(lid, 5000).sort((a, b) => a - b);
        if (!vals.length) { el.innerHTML = '<p class="text-slate-400 text-center p-8">nodata</p>'; return; }
        const n = vals.length, bw = (vals[n - 1] - vals[0]) / 50 || 1;
        const xs = [], ys = [];
        for (let i = 0; i < 100; i++) {
            const x = vals[0] + (vals[n - 1] - vals[0]) * i / 99; xs.push(x);
            let sum = 0;
            for (let j = 0; j < n; j += Math.max(1, Math.floor(n / 500))) sum += Math.exp(-0.5 * ((x - vals[j]) / bw) ** 2);
            ys.push(sum / (n * bw * Math.sqrt(2 * Math.PI)));
        }
        Plotly.newPlot(el, [{ x: xs, y: ys, type: 'scatter', mode: 'lines', fill: 'tozeroy', line: { color: layerColor(lid), width: 2 }, fillcolor: layerColor(lid).replace(')', ',0.2)').replace('rgb', 'rgba'), name: 'KDE' }], darkLayout(`KDE — ${layerLabel(lid)}`, { height: 380, xaxis: { title: { text: layerLabel(lid), font: { size: 10 } } }, yaxis: { title: { text: 'Density', font: { size: 10 } } } }), PLOTLY_CFG);
    }
    function renderCDF(lid) {
        const el = document.getElementById('chart-cdf'); if (!el) return;
        const vals = extractValues(lid, 0).sort((a, b) => a - b);
        if (!vals.length) { el.innerHTML = '<p class="text-slate-400 text-center p-8">nodata</p>'; return; }
        const step = Math.max(1, Math.floor(vals.length / 500));
        const xs = [], ys = [];
        for (let i = 0; i < vals.length; i += step) { xs.push(vals[i]); ys.push((i + 1) / vals.length); }
        Plotly.newPlot(el, [{ x: xs, y: ys, type: 'scatter', mode: 'lines', line: { color: layerColor(lid), width: 2 }, name: 'CDF' }], darkLayout(`CDF — ${layerLabel(lid)}`, { height: 380, xaxis: { title: { text: layerLabel(lid), font: { size: 10 } } }, yaxis: { title: { text: 'P(X ≤ x)', font: { size: 10 } }, range: [0, 1] } }), PLOTLY_CFG);
    }

    renderHistogram(firstId, 40);
    renderViolin(firstId);
    renderKDE(firstId);
    renderCDF(firstId);

    const histSel = document.getElementById('hist-layer-select'), binSlider = document.getElementById('hist-bins'), binLabel = document.getElementById('hist-bins-label');
    histSel?.addEventListener('change', () => renderHistogram(histSel.value, parseInt(binSlider?.value || 40)));
    binSlider?.addEventListener('input', () => { if (binLabel) binLabel.textContent = binSlider.value; renderHistogram(histSel?.value || firstId, parseInt(binSlider.value)); });
    document.getElementById('violin-layer-select')?.addEventListener('change', e => renderViolin(e.target.value));
    document.getElementById('kde-layer-select')?.addEventListener('change', e => renderKDE(e.target.value));
    document.getElementById('cdf-layer-select')?.addEventListener('change', e => renderCDF(e.target.value));

    // Box Plot — all layers
    const boxEl = document.getElementById('chart-boxplot');
    if (boxEl) {
        const traces = Object.keys(window.EDA.gridData).map(id => ({ y: extractValues(id, 3000), type: 'box', name: layerLabel(id), marker: { color: layerColor(id), size: 3 }, boxpoints: 'outliers', jitter: 0.3 }));
        if (!traces.some(t => t.y && t.y.length)) boxEl.innerHTML = '<p class="text-slate-400 text-center p-8">nodata</p>';
        else Plotly.newPlot(boxEl, traces, darkLayout('Box Plot — Tất Cả Layers', { height: 420, showlegend: false }), PLOTLY_CFG);
    }

    // Quantile
    const qEl = document.getElementById('chart-quantile');
    if (qEl) {
        const qs = [0, 0.1, 0.25, 0.5, 0.75, 0.9, 1], qL = ['Min', 'P10', 'Q1', 'Med', 'Q3', 'P90', 'Max'];
        const traces = Object.keys(window.EDA.gridData).map(id => {
            const v = extractValues(id, 0).sort((a, b) => a - b); if (!v.length) return null;
            return { x: qL, y: qs.map(q => v[Math.floor(q * (v.length - 1))]), type: 'scatter', mode: 'lines+markers', name: layerLabel(id), line: { color: layerColor(id), width: 2 }, marker: { size: 6 } };
        }).filter(Boolean);
        if (!traces.length) qEl.innerHTML = '<p class="text-slate-400 text-center p-8">nodata</p>';
        else Plotly.newPlot(qEl, traces, darkLayout('Quantile Profile', { height: 400, legend: { orientation: 'h', y: -0.18, font: { color: '#94a3b8' } } }), PLOTLY_CFG);
    }

    // ── Ridgeline ──
    const ridgeEl = document.getElementById('chart-ridgeline');
    if (ridgeEl) {
        const numL = window.LAYERS.filter(l => !l.isCat);
        const traces = [];
        numL.forEach((l, i) => {
            const vals = extractValues(l.id, 3000).sort((a, b) => a - b);
            if (!vals.length) return;
            const n = vals.length, bw = (vals[n - 1] - vals[0]) / 30 || 1;
            const xs = [], ys = [];
            for (let j = 0; j < 80; j++) {
                const x = vals[0] + (vals[n - 1] - vals[0]) * j / 79; xs.push(x);
                let sum = 0;
                for (let k = 0; k < n; k += Math.max(1, Math.floor(n / 300))) sum += Math.exp(-0.5 * ((x - vals[k]) / bw) ** 2);
                ys.push(sum / (n * bw * Math.sqrt(2 * Math.PI)) + i * 0.5);
            }
            traces.push({ x: xs, y: ys, type: 'scatter', mode: 'lines', fill: 'tozeroy', line: { color: l.color, width: 1.5 }, fillcolor: l.color + '33', name: l.label });
        });
        if (!traces.length) ridgeEl.innerHTML = '<p class="text-slate-400 text-center p-8">nodata</p>';
        else Plotly.newPlot(ridgeEl, traces, darkLayout('Ridgeline — Multi-Layer KDE', { height: 450, showlegend: true, legend: { orientation: 'h', y: -0.15, font: { color: '#94a3b8', size: 10 } } }), PLOTLY_CFG);
    }

    // ── Strip / Swarm ──
    const stripEl = document.getElementById('chart-strip');
    if (stripEl) {
        const traces = Object.keys(window.EDA.gridData).map(id => ({
            y: extractValues(id, 500), x: Array(500).fill(layerLabel(id)),
            type: 'scatter', mode: 'markers', name: layerLabel(id),
            marker: { color: layerColor(id), size: 3, opacity: 0.5 },
            jitter: 0.7
        }));
        Plotly.newPlot(stripEl, traces, darkLayout('Strip Plot — All Layers', { height: 420, showlegend: false }), PLOTLY_CFG);
    }
}

document.addEventListener('edaDataReady', renderDistributionPage);
