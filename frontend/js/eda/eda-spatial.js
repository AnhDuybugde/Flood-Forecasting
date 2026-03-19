/**
 * eda-spatial.js — Spatial Overview: 8 heatmaps + 6 new charts
 * Contour, Overlay, Choropleth, Risk Zone, Difference Map, Animated Heatmap
 */
'use strict';

function buildSpatialHeatmap(layerId, containerId) {
    const grid = window.EDA.gridData[layerId];
    if (!grid) return;
    const rows = grid.size.r, cols = grid.size.c, ds = 4;
    const dr = Math.ceil(rows / ds), dc = Math.ceil(cols / ds);
    const z = [];
    for (let r = 0; r < dr; r++) {
        const row = [];
        for (let c = 0; c < dc; c++) row.push(gridVal(grid, Math.min(r * ds, rows - 1) * cols + Math.min(c * ds, cols - 1)));
        z.push(row);
    }
    const b = grid.bounds, info = window.LAYERS.find(l => l.id === layerId);
    const cs = { rain:'Blues', soilMoisture:'YlOrBr', tide:'Teal', dem:'Earth', slope:'Hot', flow:'Purples', landCover:'Portland', label:[[0,'#1e40af'],[0.5,'#fbbf24'],[1,'#dc2626']] };
    Plotly.newPlot(containerId, [{ z, type:'heatmap', x0:b.w, dx:(b.e-b.w)/dc, y0:b.s, dy:(b.n-b.s)/dr, colorscale:cs[layerId]||'Viridis', colorbar:{title:{text:info?.unit||'',font:{size:10,color:'#94a3b8'}},thickness:12,tickfont:{color:'#94a3b8',size:9}}, hovertemplate:`Lat:%{y:.3f}<br>Lng:%{x:.3f}<br>Value:%{z:.4f}<extra>${info?.label||layerId}</extra>`, zsmooth:'best' }], darkLayout(`${info?.label||layerId} — ${info?.unit||''}`, { height:400, xaxis:{title:{text:'Longitude',font:{size:10}}}, yaxis:{title:{text:'Latitude',font:{size:10}},scaleanchor:'x'} }), PLOTLY_CFG);
}

function dsGrid(grid, ds) {
    const rows = grid.size.r, cols = grid.size.c;
    const dr = Math.ceil(rows/ds), dc = Math.ceil(cols/ds);
    const z = [];
    for (let r = 0; r < dr; r++) { const row = []; for (let c = 0; c < dc; c++) row.push(gridVal(grid, Math.min(r*ds,rows-1)*cols+Math.min(c*ds,cols-1))); z.push(row); }
    return { z, dr, dc };
}

function renderSpatialPage() {
    const container = document.getElementById('spatial-plots');
    if (!container) return;
    container.innerHTML = '';
    window.LAYERS.forEach(l => {
        const section = document.createElement('div');
        section.className = 'card';
        section.innerHTML = `<div class="card-hdr" style="cursor:pointer" onclick="this.nextElementSibling.classList.toggle('collapsed')"><div class="card-title"><span class="material-icons" style="color:${l.color}">${l.icon}</span> ${l.label} ${l.unit?'('+l.unit+')':''}</div><div style="display:flex;align-items:center;gap:6px"><span class="badge-sm">${window.EDA.gridData[l.id]?(window.EDA.gridData[l.id].size.r+'×'+window.EDA.gridData[l.id].size.c):'--'}</span><span class="material-icons" style="color:var(--text-muted);font-size:20px">expand_more</span></div></div><div class="card-body"><div id="spatial-plot-${l.id}"></div><div class="extract-panel"><div class="extract-chip"><span style="color:var(--text-muted);font-weight:600">Lat,Lng:</span><span class="extract-coord-${l.id}" style="font-family:monospace;color:${l.color};font-weight:700">Click heatmap ↑</span></div><div class="extract-chip"><span style="color:var(--text-muted);font-weight:600">Value:</span><span class="extract-val-${l.id}" style="font-family:monospace;font-weight:700">--</span></div></div></div>`;
        container.appendChild(section);
        if (window.EDA.gridData[l.id]) {
            buildSpatialHeatmap(l.id, `spatial-plot-${l.id}`);
            const plotEl = document.getElementById(`spatial-plot-${l.id}`);
            if (plotEl) plotEl.on('plotly_click', data => {
                if (!data.points?.length) return;
                const pt = data.points[0];
                const ce = document.querySelector(`.extract-coord-${l.id}`), ve = document.querySelector(`.extract-val-${l.id}`);
                if (ce) ce.textContent = `${pt.y?.toFixed(4)}, ${pt.x?.toFixed(4)}`;
                if (ve) ve.textContent = `${pt.z?.toFixed(4)} ${l.unit}`;
            });
        }
    });

    // ── Contour Map ──
    const demG = window.EDA.gridData.dem;
    if (demG) {
        const {z, dr, dc} = dsGrid(demG, 6), b = demG.bounds;
        Plotly.newPlot('spatial-contour', [{ z, type:'contour', x0:b.w, dx:(b.e-b.w)/dc, y0:b.s, dy:(b.n-b.s)/dr, colorscale:'Earth', contours:{ coloring:'heatmap', showlabels:true, labelfont:{size:9,color:'#e2e8f0'} }, line:{smoothing:0.85}, colorbar:{thickness:12,tickfont:{size:9,color:'#94a3b8'}} }], darkLayout('Contour Lines — DEM (m)', { height:400 }), PLOTLY_CFG);
    }

    // ── Overlay — DEM + Flood ──
    const lblG = window.EDA.gridData.label;
    if (demG && lblG) {
        const ds = 6, b = demG.bounds, {z:zDem, dr, dc} = dsGrid(demG, ds);
        const zFlood = [];
        for (let r = 0; r < dr; r++) { const row = []; for (let c = 0; c < dc; c++) { const v = gridVal(lblG, Math.min(r*ds,lblG.size.r-1)*lblG.size.c+Math.min(c*ds,lblG.size.c-1)); row.push(v !== null && v > 0 ? 1 : null); } zFlood.push(row); }
        Plotly.newPlot('spatial-overlay', [
            { z:zDem, type:'heatmap', x0:b.w, dx:(b.e-b.w)/dc, y0:b.s, dy:(b.n-b.s)/dr, colorscale:'Earth', showscale:true, colorbar:{x:1.02,thickness:10,tickfont:{size:8,color:'#94a3b8'}}, hovertemplate:'DEM: %{z:.1f}m<extra></extra>', zsmooth:'best' },
            { z:zFlood, type:'heatmap', x0:b.w, dx:(b.e-b.w)/dc, y0:b.s, dy:(b.n-b.s)/dr, colorscale:[[0,'rgba(0,0,0,0)'],[1,'rgba(239,68,68,0.55)']], showscale:false, hovertemplate:'Flood<extra></extra>', zsmooth:false }
        ], darkLayout('Overlay — DEM + Flood (đỏ)', { height:400 }), PLOTLY_CFG);
    }

    // ── Choropleth / Classified LULC ──
    const lcG = window.EDA.gridData.landCover;
    if (lcG) {
        const {z, dr, dc} = dsGrid(lcG, 6), b = lcG.bounds;
        const classes = [...new Set(z.flat().filter(v => v !== null))].sort((a,b)=>a-b);
        const colors = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#f97316','#ec4899','#64748b','#6366f1'];
        const dColorscale = [];
        classes.forEach((c, i) => { const t = i / Math.max(classes.length - 1, 1); dColorscale.push([t, colors[i % colors.length]]); });
        if (dColorscale.length < 2) dColorscale.push([0,'#3b82f6'],[1,'#10b981']);
        Plotly.newPlot('spatial-classified', [{ z, type:'heatmap', x0:b.w, dx:(b.e-b.w)/dc, y0:b.s, dy:(b.n-b.s)/dr, colorscale:dColorscale, zsmooth:false, colorbar:{title:{text:'Class',font:{size:10,color:'#94a3b8'}},thickness:12,tickvals:classes,tickfont:{size:8,color:'#94a3b8'}} }], darkLayout('Choropleth — Classified LULC', { height:400 }), PLOTLY_CFG);
    }

    // ── Risk Zone ──
    const rainG = window.EDA.gridData.rain;
    if (rainG && demG && lblG) {
        const ds = 5, rows = rainG.size.r, cols = rainG.size.c;
        const dr = Math.ceil(rows/ds), dc = Math.ceil(cols/ds), b = rainG.bounds;
        const z = [];
        for (let r = 0; r < dr; r++) { const row = []; for (let c = 0; c < dc; c++) {
            const idx = Math.min(r*ds,rows-1)*cols+Math.min(c*ds,cols-1);
            const rv = gridVal(rainG,idx)||0, dv = gridVal(demG,idx), fv = gridVal(lblG,idx)||0;
            let risk = 0;
            if (fv > 0) risk += 2;
            if (rv > 0.1) risk += 1;
            if (dv !== null && dv < 0.5) risk += 1;
            row.push(risk);
        } z.push(row); }
        Plotly.newPlot('spatial-riskzone', [{ z, type:'heatmap', x0:b.w, dx:(b.e-b.w)/dc, y0:b.s, dy:(b.n-b.s)/dr, colorscale:[[0,'#1e3a5f'],[0.25,'#22c55e'],[0.5,'#fbbf24'],[0.75,'#f97316'],[1,'#dc2626']], zsmooth:false, colorbar:{title:{text:'Risk Level',font:{size:10,color:'#94a3b8'}},thickness:12,tickvals:[0,1,2,3,4],ticktext:['None','Low','Med','High','Critical'],tickfont:{size:8,color:'#94a3b8'}} }], darkLayout('Risk Zone — Low DEM + High Rain + Flood', { height:400 }), PLOTLY_CFG);
    }

    // ── Difference Map stuff ──
    const diffSel = document.getElementById('diff-date-b');
    if (diffSel && window.EDA.allDates) {
        diffSel.innerHTML = '';
        window.EDA.allDates.forEach(d => { const o = document.createElement('option'); o.value = d; o.textContent = d; diffSel.appendChild(o); });
    }
}

// Difference Map loader
document.getElementById('btn-diff-load')?.addEventListener('click', async () => {
    const dateB = document.getElementById('diff-date-b')?.value;
    if (!dateB) return;
    try {
        toast('Đang tải dữ liệu ngày B...', 'info');
        const url = `/api/grid/${window.EDA.region}/${dateB}/rain?format=bin`;
        const res = await fetch(url); const buf = await res.arrayBuffer();
        const view = new DataView(buf); const ml = view.getUint32(0,true);
        const meta = JSON.parse(new TextDecoder().decode(new Uint8Array(buf,4,ml)));
        const f32B = new Float32Array(buf.slice(4+ml));
        const gridA = window.EDA.gridData.rain;
        if (!gridA) return;
        const ds = 6, rows = gridA.size.r, cols = gridA.size.c;
        const dr = Math.ceil(rows/ds), dc = Math.ceil(cols/ds), b = gridA.bounds;
        const z = [];
        for (let r = 0; r < dr; r++) { const row = []; for (let c = 0; c < dc; c++) {
            const idx = Math.min(r*ds,rows-1)*cols+Math.min(c*ds,cols-1);
            const va = gridVal(gridA,idx), scB = meta.scale||1, ndB = meta.nodata??-9999;
            const rawB = f32B[idx]; const vb = (rawB===ndB||rawB<=-9998)?null:rawB/scB;
            row.push(va!==null&&vb!==null?va-vb:null);
        } z.push(row); }
        Plotly.newPlot('spatial-difference', [{ z, type:'heatmap', x0:b.w, dx:(b.e-b.w)/dc, y0:b.s, dy:(b.n-b.s)/dr, colorscale:'RdBu', zmid:0, zsmooth:'best', colorbar:{thickness:12,tickfont:{size:9,color:'#94a3b8'}} }], darkLayout(`Rain Difference: ${window.EDA.date} − ${dateB}`, { height:400 }), PLOTLY_CFG);
        toast('✅ Difference Map ready', 'success');
    } catch(e) { console.error(e); toast('Lỗi tải ngày B', 'error'); }
});

// Animated heatmap
document.getElementById('btn-anim-play')?.addEventListener('click', async () => {
    if (!window.EDA.allDates || window.EDA.allDates.length < 3) { toast('Cần tải multi-date trước', 'error'); return; }
    const dates = window.EDA.allDates.slice(0, 10);
    const frames = [];
    toast('Đang tải animated frames...', 'info');
    const rainG = window.EDA.gridData.rain;
    if (!rainG) return;
    const ds = 8, rows = rainG.size.r, cols = rainG.size.c;
    const dr = Math.ceil(rows/ds), dc = Math.ceil(cols/ds), b = rainG.bounds;
    for (const d of dates) {
        try {
            const res = await fetch(`/api/grid/${window.EDA.region}/${d}/rain?format=bin`);
            const buf = await res.arrayBuffer(); const view = new DataView(buf);
            const ml = view.getUint32(0,true); const meta = JSON.parse(new TextDecoder().decode(new Uint8Array(buf,4,ml)));
            const f32 = new Float32Array(buf.slice(4+ml)); const sc = meta.scale||1, nd = meta.nodata??-9999;
            const z = [];
            for (let r = 0; r < dr; r++) { const row = []; for (let c = 0; c < dc; c++) { const idx = Math.min(r*ds,rows-1)*cols+Math.min(c*ds,cols-1); const raw = f32[idx]; row.push(raw===nd||raw<=-9998?null:raw/sc); } z.push(row); }
            frames.push({ z, date: d });
        } catch(e) { console.warn('Skip', d); }
    }
    if (!frames.length) return;
    const el = document.getElementById('spatial-animated'); let i = 0;
    function show() {
        const f = frames[i % frames.length];
        document.getElementById('anim-frame-label').textContent = f.date;
        Plotly.react(el, [{ z:f.z, type:'heatmap', x0:b.w, dx:(b.e-b.w)/dc, y0:b.s, dy:(b.n-b.s)/dr, colorscale:'Blues', zsmooth:'best', colorbar:{thickness:10,tickfont:{size:8,color:'#94a3b8'}} }], darkLayout(`Rain — ${f.date}`, { height:380 }), PLOTLY_CFG);
        i++; if (i < frames.length) setTimeout(show, 1200);
    }
    show();
});

document.getElementById('btn-expand-all')?.addEventListener('click', () => document.querySelectorAll('#spatial-plots .card-body').forEach(b => b.classList.remove('collapsed')));
document.getElementById('btn-collapse-all')?.addEventListener('click', () => document.querySelectorAll('#spatial-plots .card-body').forEach(b => b.classList.add('collapsed')));

document.addEventListener('edaDataReady', renderSpatialPage);
