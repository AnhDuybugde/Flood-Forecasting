/**
 * eda-datatable.js — Bảng Dữ Liệu: 6 tabs including new Cross-tabulation
 */
'use strict';

function quantile(sorted, q) { const pos = q * (sorted.length - 1); const lo = Math.floor(pos), hi = Math.ceil(pos); return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo); }

function renderDataTables() {
    const G = window.EDA.gridData;
    const allIds = Object.keys(G);
    if (!allIds.length) return;

    const dtOpts = { pageLength: 10, dom: 'Bfrtip', buttons: ['csv','excel'], language: { search: 'Tìm:', info: 'Hiển thị _START_–_END_ / _TOTAL_', paginate: { previous: '‹', next: '›' } } };

    // Summary
    const summaryEl = document.getElementById('table-summary');
    if (summaryEl && !$.fn.DataTable.isDataTable(summaryEl)) {
        const data = allIds.map(id => {
            const g = G[id], arr = extractValues(id, 0);
            if (!arr.length) return [layerLabel(id), 'nodata','nodata','nodata','nodata','nodata', 0, g.data.length];
            arr.sort((a,b)=>a-b);
            const mean = arr.reduce((a,b)=>a+b,0)/arr.length;
            let std = 0; arr.forEach(v=>std+=(v-mean)**2); std=Math.sqrt(std/arr.length);
            return [layerLabel(id), arr[0].toFixed(4), arr[arr.length-1].toFixed(4), mean.toFixed(4), quantile(arr,0.5).toFixed(4), std.toFixed(4), arr.length.toLocaleString(), (g.data.length-arr.length).toLocaleString()];
        });
        $(summaryEl).DataTable({ ...dtOpts, data, order:[[0,'asc']] });
    }

    // Percentiles
    const percEl = document.getElementById('table-percentiles');
    if (percEl && !$.fn.DataTable.isDataTable(percEl)) {
        const data = allIds.map(id => {
            const arr = extractValues(id, 0); if (!arr.length) return [layerLabel(id),'nodata','nodata','nodata','nodata','nodata','nodata','nodata','nodata'];
            arr.sort((a,b)=>a-b);
            return [layerLabel(id), arr[0].toFixed(4), quantile(arr,0.25).toFixed(4), quantile(arr,0.5).toFixed(4), quantile(arr,0.75).toFixed(4), quantile(arr,0.9).toFixed(4), quantile(arr,0.95).toFixed(4), quantile(arr,0.99).toFixed(4), arr[arr.length-1].toFixed(4)];
        });
        $(percEl).DataTable({ ...dtOpts, data });
    }

    // Correlation Matrix
    const corrHead = document.getElementById('thead-corr'), corrBody = document.getElementById('tbody-corr');
    if (corrHead && corrBody) {
        const numIds = allIds.filter(id => !window.LAYERS.find(l => l.id === id)?.isCat);
        if (numIds.length) {
            let hdr = '<tr><th>Layer</th>'; numIds.forEach(id => hdr += `<th>${layerLabel(id)}</th>`); hdr += '</tr>';
            corrHead.innerHTML = hdr;
            let body = '';
            numIds.forEach(idA => {
                body += `<tr><td><strong>${layerLabel(idA)}</strong></td>`;
                numIds.forEach(idB => {
                    const gA = G[idA], gB = G[idB]; if (!gA || !gB) { body += '<td>nodata</td>'; return; }
                    if (idA === idB) { body += '<td style="background:rgba(99,102,241,0.2);font-weight:700">1.000</td>'; return; }
                    const dA = gA.data, dB = gB.data, ndA = gA.nodata ?? -9999, ndB = gB.nodata ?? -9999, sA = gA.scale || 1, sB = gB.scale || 1;
                    let sa = 0, sb = 0, sab = 0, sa2 = 0, sb2 = 0, c = 0;
                    const step = Math.max(1, Math.floor(dA.length / 5000));
                    for (let i = 0; i < dA.length; i += step) { if (dA[i] === ndA || dA[i] <= -9998 || dB[i] === ndB || dB[i] <= -9998) continue; const a = dA[i] / sA, b = dB[i] / sB; sa += a; sb += b; sab += a * b; sa2 += a * a; sb2 += b * b; c++; }
                    const num = c * sab - sa * sb, den = Math.sqrt((c * sa2 - sa * sa) * (c * sb2 - sb * sb));
                    const r = c ? (den ? num / den : null) : null;
                    const cell = r == null ? 'nodata' : r.toFixed(3);
                    const bgOpacity = r == null ? 0 : Math.abs(r) * 0.3;
                    const bgColor = r == null ? 'transparent' : (r > 0 ? `rgba(59,130,246,${bgOpacity})` : `rgba(239,68,68,${bgOpacity})`);
                    body += `<td style="background:${bgColor}">${cell}</td>`;
                });
                body += '</tr>';
            });
            corrBody.innerHTML = body;
            if (!$.fn.DataTable.isDataTable(document.getElementById('table-correlation'))) $(document.getElementById('table-correlation')).DataTable({ ...dtOpts, paging: false });
        }
    }

    // Categorical — LULC
    const catLulc = document.getElementById('table-cat-lulc');
    if (catLulc && G.landCover && !$.fn.DataTable.isDataTable(catLulc)) {
        const counts = {}; let total = 0;
        for (let i = 0; i < G.landCover.data.length; i++) { const v = gridVal(G.landCover, i); if (v === null) continue; const c = Math.round(v); counts[c] = (counts[c] || 0) + 1; total++; }
        const data = Object.entries(counts).sort((a, b) => b[1] - a[1]).map(([cls, cnt]) => [`Class ${cls}`, cnt.toLocaleString(), (cnt / total * 100).toFixed(2) + '%']);
        $(catLulc).DataTable({ ...dtOpts, data, order: [[1, 'desc']] });
    }

    // Categorical — Flood
    const catFlood = document.getElementById('table-cat-flood');
    if (catFlood && G.label && !$.fn.DataTable.isDataTable(catFlood)) {
        let flood = 0, normal = 0;
        for (let i = 0; i < G.label.data.length; i++) { const v = gridVal(G.label, i); if (v === null) continue; v > 0 ? flood++ : normal++; }
        const total = flood + normal;
        $(catFlood).DataTable({ ...dtOpts, data: [['Flood', flood.toLocaleString(), (flood / total * 100).toFixed(2) + '%'], ['Normal', normal.toLocaleString(), (normal / total * 100).toFixed(2) + '%']] });
    }

    // ── NEW: Cross-tabulation LULC × Flood ──
    const ctEl = document.getElementById('table-crosstab');
    if (ctEl && G.landCover && G.label && !$.fn.DataTable.isDataTable(ctEl)) {
        const counts = {}; // { class: { flood: N, normal: N } }
        const step = Math.max(1, Math.floor(G.landCover.data.length / 100000));
        for (let i = 0; i < G.landCover.data.length; i += step) {
            const lc = gridVal(G.landCover, i), lv = gridVal(G.label, i);
            if (lc === null || lv === null) continue;
            const cls = Math.round(lc);
            if (!counts[cls]) counts[cls] = { flood: 0, normal: 0 };
            lv > 0 ? counts[cls].flood++ : counts[cls].normal++;
        }
        const data = Object.entries(counts).sort((a, b) => a[0] - b[0]).map(([cls, c]) => {
            const total = c.flood + c.normal;
            return [`Class ${cls}`, c.normal.toLocaleString(), c.flood.toLocaleString(), total ? (c.flood / total * 100).toFixed(1) + '%' : '0%', total.toLocaleString()];
        });
        $(ctEl).DataTable({ ...dtOpts, data, order: [[3, 'desc']] });
    }

    // Anomalies
    const anomEl = document.getElementById('table-anomalies');
    if (anomEl && G.rain && G.dem && !$.fn.DataTable.isDataTable(anomEl)) {
        const rows = G.rain.size.r, cols = G.rain.size.c, b = G.rain.bounds;
        const rArr = extractValues('rain', 0); rArr.sort((a, b2) => a - b2);
        const p95 = rArr.length ? quantile(rArr, 0.95) : 999;
        const data = [];
        for (let i = 0; i < G.rain.data.length && data.length < 100; i += Math.max(1, Math.floor(G.rain.data.length / 50000))) {
            const rv = gridVal(G.rain, i); if (rv === null || rv < p95) continue;
            const r = Math.floor(i / cols), c = i % cols;
            const lat = (b.s + (r / rows) * (b.n - b.s)).toFixed(4);
            const lng = (b.w + (c / cols) * (b.e - b.w)).toFixed(4);
            data.push([`${r},${c}`, lat, lng, rv.toFixed(3), G.dem ? (gridVal(G.dem, i) != null ? String(gridVal(G.dem, i)) : 'nodata') : 'nodata', G.tide ? (gridVal(G.tide, i) != null ? String(gridVal(G.tide, i)) : 'nodata') : 'nodata', G.flow ? (gridVal(G.flow, i) != null ? String(gridVal(G.flow, i)) : 'nodata') : 'nodata', G.label ? (gridVal(G.label, i) > 0 ? '🌊 Flood' : '✅') : 'nodata']);
        }
        $(anomEl).DataTable({ ...dtOpts, data, pageLength: 15, order: [[3, 'desc']] });
    }
}

document.addEventListener('edaDataReady', renderDataTables);
