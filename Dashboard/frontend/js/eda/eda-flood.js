/**
 * eda-flood.js — Ngập Lụt: Original 4 + 4 new charts
 * ROC-like, Feature Importance, Conditional Distribution, Decision Boundary
 */
'use strict';

function renderFloodPage() {
    const G = window.EDA.gridData;
    const lblG = G.label, rainG = G.rain, demG = G.dem;

    // Flood Binary Heatmap
    if (lblG) {
        const ds = 5, rows = lblG.size.r, cols = lblG.size.c;
        const dr = Math.ceil(rows/ds), dc = Math.ceil(cols/ds), z = [], b = lblG.bounds;
        for (let r = 0; r < dr; r++) { const row = []; for (let c = 0; c < dc; c++) { const v = gridVal(lblG, Math.min(r*ds,rows-1)*cols+Math.min(c*ds,cols-1)); row.push(v!==null&&v>0?1:0); } z.push(row); }
        Plotly.newPlot('flood-heatmap', [{ z, type:'heatmap', x0:b.w, dx:(b.e-b.w)/dc, y0:b.s, dy:(b.n-b.s)/dr, colorscale:[[0,'#1e3a5f'],[0.5,'#fbbf24'],[1,'#dc2626']], zsmooth:false, colorbar:{tickvals:[0,1],ticktext:['No','Yes'],thickness:12,tickfont:{color:'#94a3b8'}} }], darkLayout('Flood Binary Map', { height:380 }), PLOTLY_CFG);
    }

    // Pie
    if (lblG) {
        let flood = 0, normal = 0;
        for (let i = 0; i < lblG.data.length; i++) { const v = gridVal(lblG,i); if(v===null) continue; v>0?flood++:normal++; }
        Plotly.newPlot('flood-pie', [{ values:[flood,normal], labels:['Ngập Lụt 🌊','Bình Thường ✅'], type:'pie', hole:0.45, marker:{colors:['#ef4444','#3b82f6'],line:{color:'rgba(15,23,42,0.8)',width:2}}, textinfo:'label+percent', textfont:{color:'#e2e8f0',size:11} }], darkLayout('Flood vs Normal', { height:380, showlegend:true, legend:{font:{color:'#94a3b8'}} }), PLOTLY_CFG);
    }

    // Scatter rain vs DEM
    if (rainG && demG && lblG) {
        const idx = sampleIdx(1500), x=[],y=[],colors=[];
        for (const i of idx) { const r=gridVal(rainG,i),d=gridVal(demG,i),l=gridVal(lblG,i); if(r===null||d===null) continue; x.push(r);y.push(d);colors.push(l!==null&&l>0?'#ef4444':'#3b82f6'); }
        Plotly.newPlot('flood-scatter', [{ x,y, mode:'markers', type:'scatter', marker:{color:colors,size:4,opacity:0.6} }], darkLayout('Rain vs DEM (🔴Flood/🔵Normal)', { height:400, xaxis:{title:{text:'Rain (mm)',font:{size:10}}}, yaxis:{title:{text:'DEM (m)',font:{size:10}}} }), PLOTLY_CFG);
    }

    // Flood vs Normal profile
    const ids = Object.keys(G).filter(id => id !== 'label');
    if (ids.length && lblG) {
        const step = Math.max(1, Math.floor(lblG.data.length/5000));
        const fM = {}, nM = {};
        ids.forEach(id => { let fs=0,fc=0,ns=0,nc=0; const g=G[id]; for(let i=0;i<g.data.length;i+=step){ const v=gridVal(g,i),lv=gridVal(lblG,i); if(v===null||lv===null)continue; lv>0?(fs+=v,fc++):(ns+=v,nc++); } fM[id]=fc?fs/fc:0; nM[id]=nc?ns/nc:0; });
        const lbl = ids.map(id=>layerLabel(id));
        Plotly.newPlot('flood-profile', [
            { x:lbl, y:ids.map(id=>{const r=Math.max(Math.abs(fM[id]),Math.abs(nM[id]),0.001);return fM[id]/r;}), name:'Flood Mean', type:'bar', marker:{color:'#ef4444',opacity:0.85} },
            { x:lbl, y:ids.map(id=>{const r=Math.max(Math.abs(fM[id]),Math.abs(nM[id]),0.001);return nM[id]/r;}), name:'Normal Mean', type:'bar', marker:{color:'#3b82f6',opacity:0.85} }
        ], darkLayout('Flood vs Normal — Normalized Mean', { height:400, barmode:'group', legend:{orientation:'h',y:-0.18,font:{color:'#94a3b8'}} }), PLOTLY_CFG);
    }

    // ── NEW: ROC-like Curve (Flood % vs Rain Threshold) ──
    if (rainG && lblG) {
        const step = Math.max(1, Math.floor(rainG.data.length / 10000));
        const pairs = [];
        for (let i = 0; i < rainG.data.length; i += step) {
            const r = gridVal(rainG, i), l = gridVal(lblG, i);
            if (r !== null && l !== null) pairs.push({ r, f: l > 0 ? 1 : 0 });
        }
        pairs.sort((a, b) => a.r - b.r);
        const thresholds = [], tpr = [], fpr = [];
        const totalFlood = pairs.filter(p => p.f === 1).length, totalNormal = pairs.length - totalFlood;
        for (let t = 0; t <= 20; t++) {
            const thresh = (t / 20) * (pairs[pairs.length - 1]?.r || 1);
            thresholds.push(thresh);
            const above = pairs.filter(p => p.r >= thresh);
            const tp = above.filter(p => p.f === 1).length;
            const fp = above.filter(p => p.f === 0).length;
            tpr.push(totalFlood ? tp / totalFlood : 0);
            fpr.push(totalNormal ? fp / totalNormal : 0);
        }
        Plotly.newPlot('flood-roc', [
            { x: fpr, y: tpr, type: 'scatter', mode: 'lines+markers', line: { color: '#ef4444', width: 2 }, marker: { size: 5 }, name: 'ROC-like', text: thresholds.map(t => `Threshold: ${t.toFixed(2)}`), hoverinfo: 'text+x+y' },
            { x: [0, 1], y: [0, 1], type: 'scatter', mode: 'lines', line: { color: '#64748b', dash: 'dash', width: 1 }, name: 'Random', showlegend: false }
        ], darkLayout('ROC-like — Rain Threshold vs Flood Detection', { height: 400, xaxis: { title: { text: 'FPR', font: { size: 10 } }, range: [0, 1] }, yaxis: { title: { text: 'TPR', font: { size: 10 } }, range: [0, 1] } }), PLOTLY_CFG);
    }

    // ── NEW: Feature Importance ──
    if (lblG) {
        const numLayers = window.LAYERS.filter(l => !l.isCat).map(l => l.id);
        const importances = numLayers.map(id => {
            const g = G[id]; if (!g) return { id, corr: 0 };
            const d = g.data, dl = lblG.data, nd = g.nodata ?? -9999, ndl = lblG.nodata ?? -9999;
            const sc = g.scale || 1, scl = lblG.scale || 1;
            let sa = 0, sb = 0, sab = 0, sa2 = 0, sb2 = 0, c = 0;
            const step = Math.max(1, Math.floor(d.length / 5000));
            for (let i = 0; i < d.length; i += step) {
                if (d[i] === nd || d[i] <= -9998 || dl[i] === ndl || dl[i] <= -9998) continue;
                const a = d[i] / sc, b2v = dl[i] / scl;
                sa += a; sb += b2v; sab += a * b2v; sa2 += a * a; sb2 += b2v * b2v; c++;
            }
            if (!c) return { id, corr: 0 };
            const num = c * sab - sa * sb, den = Math.sqrt((c * sa2 - sa * sa) * (c * sb2 - sb * sb));
            return { id, corr: den ? Math.abs(num / den) : 0 };
        }).sort((a, b) => b.corr - a.corr);
        Plotly.newPlot('flood-feature', [{
            x: importances.map(i => i.corr), y: importances.map(i => layerLabel(i.id)),
            type: 'bar', orientation: 'h',
            marker: { color: importances.map(i => layerColor(i.id)), opacity: 0.85 },
            text: importances.map(i => i.corr.toFixed(3)), textposition: 'outside', textfont: { color: '#94a3b8', size: 10 }
        }], darkLayout('Feature Importance — |Corr with Flood|', { height: 350, margin: { l: 110, r: 60, t: 44, b: 40 }, yaxis: { autorange: 'reversed' }, xaxis: { range: [0, 1] } }), PLOTLY_CFG);
    }

    // ── NEW: Conditional Distribution ──
    if (rainG && lblG) {
        const floodRain = [], normalRain = [];
        const step = Math.max(1, Math.floor(rainG.data.length / 20000));
        for (let i = 0; i < rainG.data.length; i += step) {
            const r = gridVal(rainG, i), l = gridVal(lblG, i);
            if (r === null || l === null) continue;
            l > 0 ? floodRain.push(r) : normalRain.push(r);
        }
        Plotly.newPlot('flood-cond-dist', [
            { x: floodRain, type: 'histogram', nbinsx: 50, opacity: 0.6, marker: { color: '#ef4444' }, name: 'Rain | Flood' },
            { x: normalRain, type: 'histogram', nbinsx: 50, opacity: 0.6, marker: { color: '#3b82f6' }, name: 'Rain | Normal' }
        ], darkLayout('Conditional Distribution — P(Rain | Flood) vs P(Rain | Normal)', { height: 380, barmode: 'overlay', legend: { orientation: 'h', y: -0.15, font: { color: '#94a3b8' } } }), PLOTLY_CFG);
    }

    // ── NEW: Decision Boundary — Rain × DEM grid ──
    if (rainG && demG && lblG) {
        const step = Math.max(1, Math.floor(rainG.data.length / 8000));
        const rVals = [], dVals = [];
        for (let i = 0; i < rainG.data.length; i += step) { const r = gridVal(rainG, i), d = gridVal(demG, i); if (r !== null) rVals.push(r); if (d !== null) dVals.push(d); }
        const rMin = Math.min(...rVals), rMax = Math.max(...rVals), dMin = Math.min(...dVals), dMax = Math.max(...dVals);
        const gridRes = 40;
        const zGrid = [];
        const countGrid = Array.from({ length: gridRes }, () => Array.from({ length: gridRes }, () => ({ f: 0, n: 0 })));
        for (let i = 0; i < rainG.data.length; i += step) {
            const r = gridVal(rainG, i), d = gridVal(demG, i), l = gridVal(lblG, i);
            if (r === null || d === null || l === null) continue;
            const ri = Math.min(gridRes - 1, Math.floor((r - rMin) / (rMax - rMin + 0.001) * gridRes));
            const di = Math.min(gridRes - 1, Math.floor((d - dMin) / (dMax - dMin + 0.001) * gridRes));
            l > 0 ? countGrid[di][ri].f++ : countGrid[di][ri].n++;
        }
        for (let di = 0; di < gridRes; di++) {
            const row = [];
            for (let ri = 0; ri < gridRes; ri++) {
                const c = countGrid[di][ri];
                row.push(c.f + c.n > 0 ? c.f / (c.f + c.n) : null);
            }
            zGrid.push(row);
        }
        Plotly.newPlot('flood-decision', [{
            z: zGrid, type: 'heatmap',
            x0: rMin, dx: (rMax - rMin) / gridRes,
            y0: dMin, dy: (dMax - dMin) / gridRes,
            colorscale: [[0, '#3b82f6'], [0.5, '#fbbf24'], [1, '#ef4444']],
            colorbar: { title: { text: 'Flood Prob', font: { size: 10, color: '#94a3b8' } }, thickness: 12, tickfont: { color: '#94a3b8' } },
            hovertemplate: 'Rain: %{x:.2f}<br>DEM: %{y:.2f}<br>P(Flood): %{z:.2%}<extra></extra>'
        }], darkLayout('Decision Boundary — Rain × DEM → P(Flood)', { height: 400, xaxis: { title: { text: 'Rain (mm)', font: { size: 10 } } }, yaxis: { title: { text: 'DEM (m)', font: { size: 10 } } } }), PLOTLY_CFG);
    }
}

document.addEventListener('edaDataReady', renderFloodPage);
