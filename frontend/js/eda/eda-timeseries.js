/**
 * eda-timeseries.js — Chuỗi Thời Gian: 8 charts
 * Line, Area, Stacked Area, Calendar Heatmap, Multi-Date, Rolling Avg, Seasonal, Lag
 */
'use strict';

window.EDA.tsData = null; // { dates:[], layerMeans:{}, floodPct:[], lulcCounts:{} }

async function loadMultiDateData() {
    const nDates = parseInt(document.getElementById('ts-n-dates')?.value || '20');
    const dates = window.EDA.allDates;
    if (!dates || !dates.length) { toast('Chưa có danh sách ngày', 'error'); return; }

    const step = Math.max(1, Math.floor(dates.length / nDates));
    const selected = [];
    for (let i = 0; i < dates.length && selected.length < nDates; i += step) selected.push(dates[i]);

    const statusEl = document.getElementById('ts-load-status');
    const progressEl = document.getElementById('ts-progress');
    const fillEl = document.getElementById('ts-progress-fill');
    progressEl.style.display = 'block';

    const layerIds = ['rain', 'soilMoisture', 'tide', 'dem', 'slope', 'flow'];
    const result = { dates: [], layerMeans: {}, floodPct: [], lulcCounts: {} };
    layerIds.forEach(id => result.layerMeans[id] = []);

    for (let di = 0; di < selected.length; di++) {
        const date = selected[di];
        statusEl.textContent = `${di + 1}/${selected.length}`;
        fillEl.style.width = ((di + 1) / selected.length * 100) + '%';
        result.dates.push(date);

        for (const lid of ['rain', 'label', 'landCover', ...layerIds.filter(x => x !== 'rain')]) {
            try {
                const res = await fetch(`/api/grid/${window.EDA.region}/${date}/${lid}?format=bin`);
                if (!res.ok) continue;
                const buf = await res.arrayBuffer();
                const view = new DataView(buf);
                const ml = view.getUint32(0, true);
                const meta = JSON.parse(new TextDecoder().decode(new Uint8Array(buf, 4, ml)));
                const f32 = new Float32Array(buf.slice(4 + ml));
                const sc = meta.scale || 1, nd = meta.nodata ?? -9999;

                if (layerIds.includes(lid)) {
                    let sum = 0, count = 0;
                    const sampleStep = Math.max(1, Math.floor(f32.length / 5000));
                    for (let i = 0; i < f32.length; i += sampleStep) {
                        if (f32[i] === nd || f32[i] <= -9998) continue;
                        sum += f32[i] / sc; count++;
                    }
                    if (result.layerMeans[lid].length < di + 1) result.layerMeans[lid].push(count ? sum / count : 0);
                }
                if (lid === 'label') {
                    let flood = 0, total = 0;
                    const sampleStep = Math.max(1, Math.floor(f32.length / 10000));
                    for (let i = 0; i < f32.length; i += sampleStep) {
                        if (f32[i] === nd || f32[i] <= -9998) continue;
                        total++; if (f32[i] / sc > 0) flood++;
                    }
                    result.floodPct.push(total ? flood / total * 100 : 0);
                }
                if (lid === 'landCover') {
                    const counts = {};
                    const sampleStep = Math.max(1, Math.floor(f32.length / 10000));
                    for (let i = 0; i < f32.length; i += sampleStep) {
                        if (f32[i] === nd || f32[i] <= -9998) continue;
                        const cat = Math.round(f32[i] / sc);
                        counts[cat] = (counts[cat] || 0) + 1;
                    }
                    result.lulcCounts[date] = counts;
                }
            } catch (e) { /* skip */ }
        }
        // Pad missing means
        layerIds.forEach(id => { while (result.layerMeans[id].length <= di) result.layerMeans[id].push(0); });
    }

    progressEl.style.display = 'none';
    statusEl.textContent = `✅ ${selected.length} ngày`;
    window.EDA.tsData = result;
    toast(`✅ Đã tải ${selected.length} ngày multi-date`, 'success');
    renderTimeSeriesCharts();
}

function renderTimeSeriesCharts() {
    const ts = window.EDA.tsData;
    if (!ts || !ts.dates.length) return;

    const dates = ts.dates;

    // 1. Time Series Line
    const lineEl = document.getElementById('ts-line');
    if (lineEl) {
        const traces = Object.keys(ts.layerMeans).map(id => ({
            x: dates, y: ts.layerMeans[id], type: 'scatter', mode: 'lines+markers',
            name: layerLabel(id), line: { color: layerColor(id), width: 2 }, marker: { size: 4 }
        }));
        Plotly.newPlot(lineEl, traces, darkLayout('Mean Value Over Time', { height: 400, xaxis: { type: 'date' }, legend: { orientation: 'h', y: -0.18, font: { color: '#94a3b8', size: 10 } } }), PLOTLY_CFG);
    }

    // 2. Area — Flood %
    const areaEl = document.getElementById('ts-area');
    if (areaEl && ts.floodPct.length) {
        Plotly.newPlot(areaEl, [{ x: dates, y: ts.floodPct, type: 'scatter', mode: 'lines', fill: 'tozeroy', line: { color: '#ef4444', width: 2 }, fillcolor: 'rgba(239,68,68,0.2)', name: 'Flood %' }], darkLayout('Flood Coverage % Over Time', { height: 380, xaxis: { type: 'date' }, yaxis: { title: { text: '%', font: { size: 10 } }, range: [0, 100] } }), PLOTLY_CFG);
    }

    // 3. Stacked Area — LULC
    const saEl = document.getElementById('ts-stacked-area');
    if (saEl && Object.keys(ts.lulcCounts).length) {
        const allClasses = new Set();
        Object.values(ts.lulcCounts).forEach(c => Object.keys(c).forEach(k => allClasses.add(k)));
        const classes = [...allClasses].sort();
        const colors = ['#818cf8', '#38bdf8', '#34d399', '#fbbf24', '#f87171', '#fb923c', '#a78bfa', '#06b6d4'];
        const traces = classes.map((cls, i) => ({
            x: dates, y: dates.map(d => ts.lulcCounts[d]?.[cls] || 0),
            type: 'scatter', mode: 'lines', stackgroup: 'lulc', name: `Class ${cls}`,
            line: { color: colors[i % colors.length], width: 0 }, fillcolor: colors[i % colors.length] + 'aa'
        }));
        Plotly.newPlot(saEl, traces, darkLayout('LULC Coverage Over Time', { height: 400, xaxis: { type: 'date' }, legend: { orientation: 'h', y: -0.18, font: { color: '#94a3b8', size: 10 } } }), PLOTLY_CFG);
    }

    // 4. Calendar Heatmap
    const calEl = document.getElementById('ts-calendar');
    if (calEl && ts.layerMeans.rain) {
        const z = [ts.layerMeans.rain];
        Plotly.newPlot(calEl, [{
            x: dates, y: ['Rain'], z, type: 'heatmap', colorscale: 'Blues',
            colorbar: { title: { text: 'Mean Rain', font: { size: 10, color: '#94a3b8' } }, thickness: 12, tickfont: { color: '#94a3b8' } },
            hovertemplate: 'Date: %{x}<br>Rain: %{z:.3f}<extra></extra>'
        }], darkLayout('Calendar Heatmap — Rain', { height: 200, xaxis: { type: 'date' }, margin: { l: 60, r: 30, t: 44, b: 35 } }), PLOTLY_CFG);
    }

    // 5. Multi-Date Comparison — Rain bar
    const mdEl = document.getElementById('ts-multidate');
    if (mdEl && ts.layerMeans.rain) {
        Plotly.newPlot(mdEl, [{ x: dates, y: ts.layerMeans.rain, type: 'bar', marker: { color: dates.map((_, i) => `hsl(${i * 360 / dates.length}, 70%, 55%)`), opacity: 0.85 }, hovertemplate: '%{x}: %{y:.4f}<extra></extra>' }], darkLayout('Multi-Date Comparison — Mean Rain', { height: 380, xaxis: { type: 'date' } }), PLOTLY_CFG);
    }

    // 6. Rolling Average
    const rollEl = document.getElementById('ts-rolling');
    if (rollEl && ts.layerMeans.rain) {
        const vals = ts.layerMeans.rain, w = Math.min(7, Math.floor(vals.length / 3));
        const rolling = vals.map((_, i) => {
            const start = Math.max(0, i - w + 1);
            const slice = vals.slice(start, i + 1);
            return slice.reduce((a, b) => a + b, 0) / slice.length;
        });
        Plotly.newPlot(rollEl, [
            { x: dates, y: vals, type: 'scatter', mode: 'markers', marker: { color: '#38bdf822', size: 5 }, name: 'Raw', showlegend: true },
            { x: dates, y: rolling, type: 'scatter', mode: 'lines', line: { color: '#38bdf8', width: 3 }, name: `Rolling Avg (${w})` }
        ], darkLayout(`Rolling Average — Rain (window=${w})`, { height: 380, xaxis: { type: 'date' }, legend: { font: { color: '#94a3b8' } } }), PLOTLY_CFG);
    }

    // 7. Seasonal Decomposition (simplified: trend + residual)
    const seasEl = document.getElementById('ts-seasonal');
    if (seasEl && ts.layerMeans.rain && dates.length >= 5) {
        const vals = ts.layerMeans.rain;
        const w = Math.min(5, Math.floor(vals.length / 2));
        const trend = vals.map((_, i) => { const s = Math.max(0, i - Math.floor(w / 2)), e = Math.min(vals.length, i + Math.ceil(w / 2)); const sl = vals.slice(s, e); return sl.reduce((a, b) => a + b, 0) / sl.length; });
        const residual = vals.map((v, i) => v - trend[i]);
        Plotly.newPlot(seasEl, [
            { x: dates, y: vals, type: 'scatter', mode: 'lines', name: 'Original', line: { color: '#94a3b8', width: 1 } },
            { x: dates, y: trend, type: 'scatter', mode: 'lines', name: 'Trend', line: { color: '#38bdf8', width: 2.5 } },
            { x: dates, y: residual, type: 'bar', name: 'Residual', marker: { color: residual.map(r => r >= 0 ? '#10b981' : '#ef4444'), opacity: 0.6 } }
        ], darkLayout('Seasonal Decomposition — Rain', { height: 400, xaxis: { type: 'date' }, legend: { orientation: 'h', y: -0.15, font: { color: '#94a3b8' } } }), PLOTLY_CFG);
    }

    // 8. Lag Plot
    const lagEl = document.getElementById('ts-lag');
    if (lagEl && ts.layerMeans.rain && dates.length >= 3) {
        const vals = ts.layerMeans.rain;
        const x = vals.slice(0, -1), y = vals.slice(1);
        Plotly.newPlot(lagEl, [{ x, y, type: 'scatter', mode: 'markers', marker: { color: '#818cf8', size: 6, opacity: 0.7 }, hovertemplate: 'Rain[t]: %{x:.4f}<br>Rain[t+1]: %{y:.4f}<extra></extra>' },
            { x: [Math.min(...vals), Math.max(...vals)], y: [Math.min(...vals), Math.max(...vals)], type: 'scatter', mode: 'lines', line: { color: '#64748b', dash: 'dash', width: 1 }, showlegend: false }
        ], darkLayout('Lag Plot — Rain[t] vs Rain[t+1]', { height: 380, xaxis: { title: { text: 'Rain[t]', font: { size: 10 } } }, yaxis: { title: { text: 'Rain[t+1]', font: { size: 10 } } } }), PLOTLY_CFG);
    }
}

document.getElementById('btn-ts-load')?.addEventListener('click', loadMultiDateData);
