/**
 * eda-landcover.js — Thảm Phủ: Original 4 + 5 new charts
 * Stacked Bar, Grouped Bar, Sunburst, Mosaic, Waffle
 */
'use strict';

function renderLandCoverPage() {
    const lcG = window.EDA.gridData.landCover, lblG = window.EDA.gridData.label;
    if (!lcG) return;
    const G = window.EDA.gridData;

    // Count per class
    const counts = {}, floodCounts = {};
    for (let i = 0; i < lcG.data.length; i++) {
        const v = gridVal(lcG, i); if (v === null) continue;
        const cat = Math.round(v);
        counts[cat] = (counts[cat] || 0) + 1;
        if (lblG) { const lv = gridVal(lblG, i); if (lv !== null && lv > 0) floodCounts[cat] = (floodCounts[cat] || 0) + 1; }
    }
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    const labels = sorted.map(([c]) => `Class ${c}`), vals = sorted.map(([, v]) => v);
    const pieColors = ['#818cf8','#38bdf8','#34d399','#fbbf24','#f87171','#fb923c','#a78bfa','#06b6d4','#f43f5e','#94a3b8'];

    // LULC Heatmap
    const rows = lcG.size.r, cols = lcG.size.c, ds = 6;
    const dr = Math.ceil(rows/ds), dc = Math.ceil(cols/ds), z = [];
    for (let r = 0; r < dr; r++) { const row = []; for (let c = 0; c < dc; c++) row.push(gridVal(lcG, Math.min(r*ds,rows-1)*cols+Math.min(c*ds,cols-1))); z.push(row); }
    const b = lcG.bounds;
    Plotly.newPlot('lc-heatmap', [{ z, type:'heatmap', x0:b.w, dx:(b.e-b.w)/dc, y0:b.s, dy:(b.n-b.s)/dr, colorscale:'Portland', zsmooth:false, colorbar:{title:{text:'Class',font:{size:10,color:'#94a3b8'}},thickness:12,tickfont:{color:'#94a3b8'}} }], darkLayout('LULC Map', { height:380 }), PLOTLY_CFG);

    // Pie
    Plotly.newPlot('lc-pie', [{ values:vals, labels, type:'pie', hole:0.4, marker:{colors:pieColors,line:{color:'rgba(15,23,42,0.8)',width:2}}, textinfo:'label+percent', textfont:{color:'#e2e8f0',size:10} }], darkLayout('LULC Distribution', { height:380, showlegend:false, margin:{l:20,r:20,t:44,b:20} }), PLOTLY_CFG);

    // Bar
    Plotly.newPlot('lc-bar', [{ x:vals, y:labels, type:'bar', orientation:'h', marker:{color:'#818cf8',opacity:0.85}, text:vals.map(v=>v.toLocaleString()), textposition:'outside', textfont:{color:'#94a3b8',size:10} }], darkLayout('', { height:Math.max(300,sorted.length*36+80), margin:{l:80,r:70,t:10,b:40}, yaxis:{autorange:'reversed'} }), PLOTLY_CFG);

    // Treemap
    const ids = ['LULC', ...Object.keys(counts).map(c=>`Class ${c}`)];
    const parents = ['', ...Object.keys(counts).map(()=>'LULC')];
    const tmVals = [0, ...Object.keys(counts).map(c=>counts[c])];
    const tmLabels = ['Land Cover', ...Object.keys(counts).map(cat => { const pct = ((counts[cat]/total)*100).toFixed(1); const fp = floodCounts[cat]?((floodCounts[cat]/counts[cat])*100).toFixed(0):'0'; return `Class ${cat}<br>${pct}%<br>🌊${fp}%`; })];
    Plotly.newPlot('lc-treemap', [{ type:'treemap', ids, parents, values:tmVals, labels:tmLabels, textinfo:'label+value', marker:{colorscale:'Teal',line:{width:2,color:'rgba(15,23,42,0.8)'}}, branchvalues:'total', textfont:{color:'#e2e8f0'} }], darkLayout('Treemap', { height:420, margin:{l:10,r:10,t:44,b:10} }), PLOTLY_CFG);

    // ── NEW: Stacked Bar — Flood vs Non-Flood per class ──
    const catKeys = sorted.map(([c]) => c);
    const nonFlood = catKeys.map(c => (counts[c]||0)-(floodCounts[c]||0));
    const flood = catKeys.map(c => floodCounts[c]||0);
    Plotly.newPlot('lc-stacked-bar', [
        { y:labels, x:nonFlood, type:'bar', orientation:'h', name:'Non-Flood', marker:{color:'#3b82f6',opacity:0.85} },
        { y:labels, x:flood, type:'bar', orientation:'h', name:'Flood', marker:{color:'#ef4444',opacity:0.85} }
    ], darkLayout('Stacked — Flood/Non-Flood per LULC', { height:Math.max(300,sorted.length*40+80), barmode:'stack', yaxis:{autorange:'reversed'}, legend:{orientation:'h',y:-0.12,font:{color:'#94a3b8'}} }), PLOTLY_CFG);

    // ── NEW: Grouped Bar — Mean values per class ──
    const numLayers = ['rain','soilMoisture','tide','dem','slope','flow'];
    const meansPerClassLayer = {};
    numLayers.forEach(id => { meansPerClassLayer[id] = {}; catKeys.forEach(c => meansPerClassLayer[id][c] = { sum: 0, count: 0 }); });
    const step = Math.max(1, Math.floor(lcG.data.length / 20000));
    for (let i = 0; i < lcG.data.length; i += step) {
        const lc = gridVal(lcG, i); if (lc === null) continue;
        const cat = String(Math.round(lc));
        numLayers.forEach(id => { const g = G[id]; if (!g) return; const v = gridVal(g, i); if (v !== null) { meansPerClassLayer[id][cat].sum += v; meansPerClassLayer[id][cat].count++; } });
    }
    const gTraces = numLayers.filter(id => G[id]).map(id => ({
        x: catKeys.map(c => `Class ${c}`),
        y: catKeys.map(c => { const m = meansPerClassLayer[id][c]; return m.count ? m.sum / m.count : 0; }),
        type: 'bar', name: layerLabel(id), marker: { color: layerColor(id), opacity: 0.85 }
    }));
    Plotly.newPlot('lc-grouped-bar', gTraces, darkLayout('Mean per LULC Class', { height:400, barmode:'group', legend:{orientation:'h',y:-0.15,font:{color:'#94a3b8',size:10}} }), PLOTLY_CFG);

    // ── NEW: Sunburst ──
    const sbIds = ['root'], sbLabels = ['LULC'], sbParents = [''], sbValues = [0];
    catKeys.forEach(c => {
        const fC = floodCounts[c] || 0, nC = counts[c] - fC;
        sbIds.push(`c${c}`); sbLabels.push(`Class ${c}`); sbParents.push('root'); sbValues.push(counts[c]);
        sbIds.push(`c${c}_f`); sbLabels.push(`Flood`); sbParents.push(`c${c}`); sbValues.push(fC);
        sbIds.push(`c${c}_n`); sbLabels.push(`Normal`); sbParents.push(`c${c}`); sbValues.push(nC);
    });
    Plotly.newPlot('lc-sunburst', [{ type:'sunburst', ids:sbIds, labels:sbLabels, parents:sbParents, values:sbValues, branchvalues:'total', marker:{colors:sbLabels.map((_,i) => pieColors[i%pieColors.length]),line:{width:1,color:'rgba(15,23,42,0.8)'}}, textfont:{color:'#e2e8f0',size:10} }], darkLayout('Sunburst — LULC × Flood', { height:420, margin:{l:10,r:10,t:44,b:10} }), PLOTLY_CFG);

    // ── NEW: Mosaic ──
    const mX = [], mY = [], mText = [], mColors = [];
    let cumX = 0;
    catKeys.forEach((c, ci) => {
        const w = counts[c] / total;
        const fR = (floodCounts[c] || 0) / counts[c];
        mX.push(cumX + w/2); mY.push(fR); mText.push(`Class ${c}<br>Total: ${counts[c].toLocaleString()}<br>Flood: ${(fR*100).toFixed(1)}%`);
        mColors.push(pieColors[ci % pieColors.length]);
        cumX += w;
    });
    Plotly.newPlot('lc-mosaic', [{
        x: mX, y: mY, type: 'bar', text: mText, hoverinfo: 'text',
        width: catKeys.map(c => counts[c] / total * 0.95),
        marker: { color: mColors, opacity: 0.85, line: { color: 'rgba(255,255,255,0.2)', width: 1 } }
    }], darkLayout('Mosaic — Width=Coverage, Height=Flood%', { height: 380, xaxis: { title: { text: 'Coverage', font: { size: 10 } }, range: [0, 1] }, yaxis: { title: { text: 'Flood Rate', font: { size: 10 } }, range: [0, 1] } }), PLOTLY_CFG);

    // ── NEW: Waffle Chart ──
    const wEl = document.getElementById('lc-waffle');
    if (wEl) {
        const gridSize = 400; // 20x20 = 400 cells
        let html = '<div class="waffle-grid">';
        const portions = catKeys.map(c => ({ cat: c, n: Math.max(1, Math.round(counts[c] / total * gridSize)) }));
        let remaining = gridSize - portions.reduce((s, p) => s + p.n, 0);
        if (remaining > 0) portions[0].n += remaining;
        let cellIdx = 0;
        portions.forEach((p, pi) => {
            for (let j = 0; j < p.n && cellIdx < gridSize; j++, cellIdx++) {
                html += `<div class="waffle-cell" style="background:${pieColors[pi%pieColors.length]}" title="Class ${p.cat}: ${((counts[p.cat]/total)*100).toFixed(1)}%"></div>`;
            }
        });
        html += '</div>';
        html += '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:12px;justify-content:center">';
        catKeys.forEach((c, i) => { html += `<span style="display:flex;align-items:center;gap:4px;font-size:11px;color:var(--text-dim)"><span style="width:12px;height:12px;border-radius:3px;background:${pieColors[i%pieColors.length]}"></span>Class ${c} (${((counts[c]/total)*100).toFixed(1)}%)</span>`; });
        html += '</div>';
        wEl.innerHTML = html;
    }
}

document.addEventListener('edaDataReady', renderLandCoverPage);
