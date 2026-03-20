/**
 * eda-tables.js — DataTables Analytics for Grid EDA
 * Listens for 'edaDataLoaded' event dispatched by eda.js.
 */

// ── Globals & UI ──
let dataTables = {};
window.edaDataTables = dataTables;

document.addEventListener('edaDataLoaded', () => {
    console.log('[EDA Tables] Data loaded triggered. Computing table analytics...');
    computeAndRenderTables();
});

// ── Analytics Computation Core ──
function computeAndRenderTables() {
    try {
        if (!window.gridData || Object.keys(window.gridData).length === 0) return;

    // Destroy existing DataTables instances if re-rendering
    Object.values(dataTables).forEach(dt => dt.destroy());
    dataTables = {};

    const summaryData = [];
    const percentileData = [];
    let lulcCounts = {};
    let labelCounts = {};

    const layerKeys = Object.keys(window.gridData);
    if (!layerKeys.length) return;

    const sizeR = window.gridData[layerKeys[0]].size.r;
    const sizeC = window.gridData[layerKeys[0]].size.c;
    const totalPixels = sizeR * sizeC;

    // 1. Process layers one by one to avoid massive memory duplication
    layerKeys.forEach(layerId => {
        const grid = window.gridData[layerId];
        const data = grid.data;
        const s = grid.scale || 1;
        const nodata = grid.nodata ?? -9999;
        const layerInfo = window.LAYERS.find(l => l.id === layerId);
        const lName = layerInfo ? layerInfo.label : layerId;
        
        // Arrays for numeric aggregation
        let validValues = [];
        let min = Infinity, max = -Infinity, sum = 0, count = 0, missing = 0;

        for (let i = 0; i < totalPixels; i++) {
            const raw = data[i];
            if (raw === nodata || raw <= -9998 || raw == null) {
                missing++;
                continue;
            }
            const v = raw / s;
            validValues.push(v);
            min = Math.min(min, v);
            max = Math.max(max, v);
            sum += v;
            count++;

            // Categorical tracking
            if (layerId === 'landCover') {
                lulcCounts[v] = (lulcCounts[v] || 0) + 1;
            } else if (layerId === 'label') {
                const cat = v > 0 ? 1 : 0;
                labelCounts[cat] = (labelCounts[cat] || 0) + 1;
            }
        }

        if (count > 0) {
            const mean = sum / count;
            // standard dev
            let sqSum = 0;
            for (let i = 0; i < count; i++) sqSum += Math.pow(validValues[i] - mean, 2);
            const stdDev = Math.sqrt(sqSum / count);

            // Sorting for percentiles
            validValues.sort((a, b) => a - b);
            
            const getPercentile = (p) => validValues[Math.floor((count - 1) * p)];
            const median = getPercentile(0.5);

            summaryData.push([
                lName,
                min.toFixed(4),
                max.toFixed(4),
                mean.toFixed(4),
                median.toFixed(4),
                stdDev.toFixed(4),
                count.toLocaleString(),
                missing.toLocaleString()
            ]);

            percentileData.push([
                lName,
                min.toFixed(4),
                getPercentile(0.25).toFixed(4),
                median.toFixed(4),
                getPercentile(0.75).toFixed(4),
                getPercentile(0.90).toFixed(4),
                getPercentile(0.95).toFixed(4),
                getPercentile(0.99).toFixed(4),
                max.toFixed(4)
            ]);
        }
    });

    // 2. Correlation Matrix
    const numericLayers = window.LAYERS.filter(l => !l.isCat).map(l => l.id);
    const corrData = [];
    const theadCorr = document.getElementById('thead-correlation').querySelector('tr');
    theadCorr.innerHTML = '<th>Layer</th>';
    numericLayers.forEach(lId => {
        const info = window.LAYERS.find(l => l.id === lId);
        theadCorr.innerHTML += `<th>${info ? info.id : lId}</th>`; // Short names
    });

    numericLayers.forEach(layerY => {
        const rowData = [window.LAYERS.find(l => l.id === layerY)?.label || layerY]; // Row header
        numericLayers.forEach(layerX => {
            if (layerY === layerX) {
                rowData.push(1.0);
            } else {
                rowData.push(getPearson(window.gridData[layerY], window.gridData[layerX]));
            }
        });
        corrData.push(rowData);
    });

    // 3. Top Anomalies (Focus on places with high rain or flood label == 1)
    const anomalyData = [];
    // Only capture top 500 anomalies to render fast
    const bounds = window.gridData[layerKeys[0]].bounds;
    const latStep = (bounds.n - bounds.s) / sizeR;
    const lngStep = (bounds.e - bounds.w) / sizeC;

    const labelGrid = window.gridData['label'];
    const rainGrid = window.gridData['rain'];
    const demGrid = window.gridData['dem'];
    const tideGrid = window.gridData['tide'];
    const flowGrid = window.gridData['flow'];

    if (labelGrid && rainGrid) {
        // Collect points where label == 1 or rain is in top percentiles (heuristically > 50mm if scale 1)
        let anomaliesFound = [];
        for (let r = 0; r < sizeR; r++) {
            for (let c = 0; c < sizeC; c++) {
                const idx = r * sizeC + c;
                const isFlood = labelGrid.data[idx] > 0;
                const rainVal = (rainGrid.data[idx] / (rainGrid.scale||1));
                
                // We'll collect all flood pixels, or high rain pixels. 
                if (isFlood || rainVal > 50) {
                    const demV = demGrid ? (demGrid.data[idx]/(demGrid.scale||1)) : null;
                    const tideV = tideGrid ? (tideGrid.data[idx]/(tideGrid.scale||1)) : null;
                    const flowV = flowGrid ? (flowGrid.data[idx]/(flowGrid.scale||1)) : null;
                    
                    const lat = (bounds.n - (r + 0.5) * latStep).toFixed(4);
                    const lng = (bounds.w + (c + 0.5) * lngStep).toFixed(4);
                    
                    anomaliesFound.push({
                        idx, r, c, lat, lng, 
                        isFlood: isFlood ? "Yes (1)" : "No (0)",
                        rain: rainVal > -999 ? rainVal.toFixed(2) : "nodata",
                        dem: demV != null && demV > -999 ? demV.toFixed(2) : "nodata",
                        tide: tideV != null && tideV > -999 ? tideV.toFixed(2) : "nodata",
                        flow: flowV != null && flowV > -999 ? flowV.toFixed(0) : "nodata"
                    });
                }
            }
        }
        
        // Sort by rain descending, take top 500
        anomaliesFound.sort((a,b) => parseFloat(b.rain) - parseFloat(a.rain));
        anomaliesFound = anomaliesFound.slice(0, 500);

        anomaliesFound.forEach(a => {
            anomalyData.push([
                `[${a.r}, ${a.c}]`,
                a.lat,
                a.lng,
                a.rain,
                a.dem,
                a.tide,
                a.flow,
                a.isFlood
            ]);
        });
    }

    // ── Render DataTables ──
    const dtOptions = {
        dom: 'Bfrtip',
        pageLength: 10,
        buttons: ['copy', 'csv', 'excel'],
        scrollX: true,
        destroy: true
    };

    // Table 1: Summary
    const tbodySummary = document.getElementById('table-summary').querySelector('tbody');
    tbodySummary.innerHTML = summaryData.map(r => `<tr>${r.map((c, i) => i === 0 ? `<td class="font-bold whitespace-nowrap">${c}</td>` : `<td>${c}</td>`).join('')}</tr>`).join('');
    dataTables.summary = $('#table-summary').DataTable({ ...dtOptions, order: [[1, 'asc']] });

    // Table 2: Percentiles
    const tbodyPerc = document.getElementById('table-percentiles').querySelector('tbody');
    tbodyPerc.innerHTML = percentileData.map(r => `<tr>${r.map((c, i) => i === 0 ? `<td class="font-bold whitespace-nowrap">${c}</td>` : `<td>${c}</td>`).join('')}</tr>`).join('');
    dataTables.percentiles = $('#table-percentiles').DataTable({ ...dtOptions, order: [[0, 'asc']] });

    // Table 3: Correlation
    const tbodyCorr = document.getElementById('tbody-correlation');
    tbodyCorr.innerHTML = corrData.map(r => `<tr>${r.map((c, i) => {
        if (i===0) return `<td class="font-bold whitespace-nowrap">${c}</td>`;
        if (c === "nodata" || c === "N/A" || typeof c !== "number" || isNaN(c)) return `<td class="text-center text-slate-400 font-mono">nodata</td>`;
        
        const v = parseFloat(c);
        let bgStyle = '';
        if (v > 0) bgStyle = `background: rgba(239, 68, 68, ${Math.abs(v)}); color: ${Math.abs(v) > 0.5 ? 'white' : 'black'};`;
        else if (v < 0) bgStyle = `background: rgba(59, 130, 246, ${Math.abs(v)}); color: ${Math.abs(v) > 0.5 ? 'white' : 'black'};`;
        return `<td style="${bgStyle}" class="text-center font-mono">${v.toFixed(3)}</td>`;
    }).join('')}</tr>`).join('');
    dataTables.correlation = $('#table-correlation').DataTable({ ...dtOptions, paging: false, dom: 'Brtip' });

    // Table 4: Categorical
    const tbodyLulc = document.getElementById('table-cat-lulc').querySelector('tbody');
    tbodyLulc.innerHTML = Object.entries(lulcCounts).map(([cat, count]) => {
        const pct = ((count / totalPixels)*100).toFixed(2);
        return `<tr><td class="font-bold">Class ${cat}</td><td>${count.toLocaleString()}</td><td>${pct}%</td></tr>`;
    }).join('');
    dataTables.catLulc = $('#table-cat-lulc').DataTable({ ...dtOptions, order: [[1, 'desc']], dom: 'rtip' });

    const tbodyFlood = document.getElementById('table-cat-flood').querySelector('tbody');
    tbodyFlood.innerHTML = Object.entries(labelCounts).map(([cat, count]) => {
        const pct = ((count / totalPixels)*100).toFixed(2);
        const name = cat == 1 ? "Ngập lụt (>0)" : "Bình thường (<=0)";
        return `<tr><td class="font-bold ${cat==1?'text-red-600':''}">${name}</td><td>${count.toLocaleString()}</td><td>${pct}%</td></tr>`;
    }).join('');
    dataTables.catFlood = $('#table-cat-flood').DataTable({ ...dtOptions, order: [[1, 'desc']], dom: 'rtip' });

    // Table 5: Anomalies
    const tbodyAnomalies = document.getElementById('table-anomalies').querySelector('tbody');
    tbodyAnomalies.innerHTML = anomalyData.map(r => `<tr>${r.map(c => `<td>${c}</td>`).join('')}</tr>`).join('');
    dataTables.anomalies = $('#table-anomalies').DataTable({ ...dtOptions, order: [[3, 'desc']] });

    // Adjust columns on first init since tabs might hide them
    setTimeout(() => { Object.values(dataTables).forEach(dt => dt.columns && dt.columns.adjust()); }, 200);
    window.edaDataTables = dataTables;
    } catch (err) {
        console.error("DataTables Rendering Error:", err);
        if (typeof toast === 'function') toast("Lỗi xử lý bảng dữ liệu: " + err.message, "error");
    }
}

// Helper: Calculate Pearson Correlation between two grid arrays
function getPearson(gridA, gridB) {
    if (!gridA || !gridB) return "nodata";
    const dataA = gridA.data, dataB = gridB.data;
    const nodataA = gridA.nodata ?? -9999, nodataB = gridB.nodata ?? -9999;
    const sA = gridA.scale||1, sB = gridB.scale||1;
    
    let sumA = 0, sumB = 0, sumAB = 0, sumA2 = 0, sumB2 = 0, count = 0;
    
    for(let i=0; i<dataA.length; i++) {
        const rawA = dataA[i], rawB = dataB[i];
        if (rawA === nodataA || rawA <= -9998 || rawA == null ||
            rawB === nodataB || rawB <= -9998 || rawB == null) continue;
            
        const a = rawA / sA, b = rawB / sB;
        sumA += a;
        sumB += b;
        sumAB += a * b;
        sumA2 += a * a;
        sumB2 += b * b;
        count++;
    }
    
    if (count === 0) return "nodata";
    
    const num = count * sumAB - sumA * sumB;
    const den = Math.sqrt((count * sumA2 - sumA * sumA) * (count * sumB2 - sumB * sumB));
    
    if (den === 0) return 0;
    return num / den;
}
