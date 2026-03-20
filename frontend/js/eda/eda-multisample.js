/**
 * eda-multisample.js — Multi-Sample Compare
 * Load 2 dates, compute per-channel difference maps, and display change statistics.
 */
'use strict';

function initMultiSamplePage() {
    const selA = document.getElementById('ms-date-a');
    const selB = document.getElementById('ms-date-b');
    if (!selA || !selB || !window.EDA.allDates) return;

    // Populate date selectors
    [selA, selB].forEach(sel => {
        sel.innerHTML = '';
        window.EDA.allDates.forEach(d => {
            const opt = document.createElement('option');
            opt.value = d; opt.textContent = d;
            sel.appendChild(opt);
        });
    });

    // Default: A = current loaded date, B = first available different date
    if (window.EDA.date) selA.value = window.EDA.date;
    const diffDate = window.EDA.allDates.find(d => d !== window.EDA.date);
    if (diffDate) selB.value = diffDate;
}

async function loadGridBinForDate(region, date, layer) {
    const url = `/api/grid/${region}/${date}/${layer}?format=bin`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Grid ${layer}/${date} failed: ${res.status}`);
    const buf = await res.arrayBuffer();
    const view = new DataView(buf);
    const metaLen = view.getUint32(0, true);
    const metaStr = new TextDecoder().decode(new Uint8Array(buf, 4, metaLen));
    const meta = JSON.parse(metaStr);
    const dataBuf = buf.slice(4 + metaLen);
    const f32 = new Float32Array(dataBuf);
    return { ...meta, data: f32 };
}

document.getElementById('btn-ms-compare')?.addEventListener('click', async () => {
    const dateA = document.getElementById('ms-date-a')?.value;
    const dateB = document.getElementById('ms-date-b')?.value;
    if (!dateA || !dateB) { toast('Hãy chọn 2 ngày', 'error'); return; }
    if (dateA === dateB) { toast('2 ngày phải khác nhau', 'error'); return; }

    const region = window.EDA.region;
    const layerIds = window.LAYERS.filter(l => !l.isCat).map(l => l.id);
    const progress = document.getElementById('ms-progress');
    const progressFill = document.getElementById('ms-progress-fill');
    const container = document.getElementById('ms-diff-maps');
    const statsEl = document.getElementById('ms-stats-chart');

    progress.style.display = 'block';
    container.innerHTML = '';
    toast(`Đang tải dữ liệu ${dateA} vs ${dateB}...`, 'info');

    const gridDataA = {};
    const gridDataB = {};
    let loaded = 0;
    const total = layerIds.length * 2;

    // Load all layers for both dates
    for (const lid of layerIds) {
        try {
            gridDataA[lid] = await loadGridBinForDate(region, dateA, lid);
        } catch (e) { console.warn(`Skip A/${lid}:`, e.message); }
        loaded++;
        progressFill.style.width = Math.round(loaded / total * 100) + '%';

        try {
            gridDataB[lid] = await loadGridBinForDate(region, dateB, lid);
        } catch (e) { console.warn(`Skip B/${lid}:`, e.message); }
        loaded++;
        progressFill.style.width = Math.round(loaded / total * 100) + '%';
    }

    progress.style.display = 'none';

    // Render difference maps
    const changeStats = [];

    layerIds.forEach(lid => {
        const gA = gridDataA[lid], gB = gridDataB[lid];
        if (!gA || !gB) return;

        const info = window.LAYERS.find(l => l.id === lid);
        const ds = 6, rows = gA.size.r, cols = gA.size.c;
        const dr = Math.ceil(rows / ds), dc = Math.ceil(cols / ds);
        const b = gA.bounds;
        const ndA = gA.nodata ?? -9999, ndB = gB.nodata ?? -9999;
        const scA = gA.scale || 1, scB = gB.scale || 1;

        const z = [];
        let sumDiff = 0, sumAbsDiff = 0, diffCount = 0;
        let maxIncrease = 0, maxDecrease = 0;

        for (let r = 0; r < dr; r++) {
            const row = [];
            for (let c = 0; c < dc; c++) {
                const idx = Math.min(r * ds, rows - 1) * cols + Math.min(c * ds, cols - 1);
                const rawA = gA.data[idx], rawB = gB.data[idx];
                if (rawA === ndA || rawA <= -9998 || rawB === ndB || rawB <= -9998) {
                    row.push(null);
                    continue;
                }
                const va = rawA / scA, vb = rawB / scB;
                const diff = va - vb;
                row.push(diff);
                sumDiff += diff;
                sumAbsDiff += Math.abs(diff);
                diffCount++;
                if (diff > maxIncrease) maxIncrease = diff;
                if (diff < maxDecrease) maxDecrease = diff;
            }
            z.push(row);
        }

        const meanDiff = diffCount > 0 ? sumDiff / diffCount : 0;
        const meanAbsDiff = diffCount > 0 ? sumAbsDiff / diffCount : 0;

        changeStats.push({
            id: lid,
            label: info?.label || lid,
            color: info?.color || '#64748b',
            meanDiff,
            meanAbsDiff,
            maxIncrease,
            maxDecrease
        });

        // Create diff map card
        const card = document.createElement('div');
        card.className = 'card';
        const plotId = `ms-diff-${lid}`;
        card.innerHTML = `
        <div class="card-hdr">
            <div class="card-title">
                <span class="material-icons" style="color:${info?.color || '#64748b'}">${info?.icon || 'layers'}</span>
                ${info?.label || lid} — Difference (A − B)
            </div>
            <div style="display:flex;gap:8px;align-items:center">
                <span class="badge-sm" style="background:${meanDiff >= 0 ? 'rgba(16,185,129,0.2)' : 'rgba(239,68,68,0.2)'};color:${meanDiff >= 0 ? '#34d399' : '#f87171'}">Mean: ${meanDiff >= 0 ? '+' : ''}${meanDiff.toFixed(4)}</span>
                <span class="badge-sm">|Mean|: ${meanAbsDiff.toFixed(4)}</span>
            </div>
        </div>
        <div class="card-body"><div id="${plotId}"></div></div>`;
        container.appendChild(card);

        Plotly.newPlot(plotId, [{
            z, type: 'heatmap',
            x0: b.w, dx: (b.e - b.w) / dc,
            y0:b.n, dy:-(b.n-b.s) / dr,
            colorscale: 'RdBu', zmid: 0, zsmooth: 'best',
            colorbar: { thickness: 12, tickfont: { size: 9, color: '#94a3b8' } },
            hovertemplate: `${info?.label}: %{z:.4f}<br>Lat:%{y:.3f} Lng:%{x:.3f}<extra>Δ(A-B)</extra>`
        }], darkLayout(`${info?.label} — ${dateA} minus ${dateB}`, {
            height: 380,
            xaxis: { title: { text: 'Lng', font: { size: 10 } } },
            yaxis: { title: { text: 'Lat', font: { size: 10 } }, scaleanchor: 'x' }
        }), PLOTLY_CFG);
    });

    // Change statistics bar chart
    if (changeStats.length) {
        Plotly.newPlot(statsEl, [
            {
                x: changeStats.map(s => s.label),
                y: changeStats.map(s => s.meanAbsDiff),
                name: '|Mean Diff|',
                type: 'bar',
                marker: { color: changeStats.map(s => s.color), opacity: 0.85 },
                text: changeStats.map(s => s.meanAbsDiff.toFixed(4)),
                textposition: 'outside',
                textfont: { size: 10, color: '#94a3b8' }
            }
        ], darkLayout(`Change Magnitude — ${dateA} vs ${dateB}`, {
            height: 350,
            margin: { l: 50, r: 30, t: 44, b: 80 },
            xaxis: { tickangle: -30 }
        }), PLOTLY_CFG);
    }

    toast(`✅ So sánh hoàn tất: ${changeStats.length} kênh`, 'success');
});

// Also build a flood diff overlay
document.addEventListener('edaDataReady', initMultiSamplePage);
