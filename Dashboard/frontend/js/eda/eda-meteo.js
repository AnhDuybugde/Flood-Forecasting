/**
 * eda-meteo.js — Sub-page 2: Khí Tượng — Rain, Tide, Soil Moisture
 */
'use strict';

function miniHeatmap(layerId, divId) {
    const grid = window.EDA.gridData[layerId];
    if (!grid) { document.getElementById(divId).innerHTML = '<p style="color:var(--text-muted);text-align:center;padding:40px">nodata</p>'; return; }

    const rows = grid.size.r, cols = grid.size.c, ds = 6;
    const dr = Math.ceil(rows / ds), dc = Math.ceil(cols / ds);
    const z = [];
    for (let r = 0; r < dr; r++) {
        const row = [];
        for (let c = 0; c < dc; c++) row.push(gridVal(grid, Math.min(r * ds, rows - 1) * cols + Math.min(c * ds, cols - 1)));
        z.push(row);
    }
    const b = grid.bounds;
    const cscale = { rain: 'Blues', tide: 'Teal', soilMoisture: 'YlOrBr' };

    Plotly.newPlot(divId, [{
        z, type: 'heatmap', x0: b.w, dx: (b.e - b.w) / dc, y0: b.s, dy: (b.n - b.s) / dr,
        colorscale: cscale[layerId] || 'Viridis', zsmooth: 'best',
        colorbar: { thickness: 10, tickfont: { size: 8, color: '#94a3b8' } },
        hovertemplate: `%{z:.3f} ${layerUnit(layerId)}<extra></extra>`,
    }], darkLayout('', { height: 280, margin: { l: 45, r: 10, t: 10, b: 35 } }), window.PLOTLY_CFG);
}

function renderMeteoPage() {
    miniHeatmap('rain', 'meteo-rain-heatmap');
    miniHeatmap('tide', 'meteo-tide-heatmap');
    miniHeatmap('soilMoisture', 'meteo-soil-heatmap');

    // Overlaid histogram
    const histEl = document.getElementById('meteo-histogram');
    if (histEl) {
        const traces = ['rain', 'soilMoisture', 'tide'].map(id => ({
            x: extractValues(id, 5000),
            type: 'histogram', nbinsx: 50, opacity: 0.65,
            marker: { color: layerColor(id) },
            name: layerLabel(id),
        }));
        Plotly.newPlot(histEl, traces, darkLayout('Histogram So Sánh 3 Biến Khí Tượng', {
            barmode: 'overlay', height: 380,
            xaxis: { title: { text: 'Giá trị', font: { size: 10 } } },
            yaxis: { title: { text: 'Pixel Count', font: { size: 10 } } },
            legend: { orientation: 'h', y: -0.15, font: { color: '#94a3b8' } },
        }), window.PLOTLY_CFG);
    }

    // Box plot
    const boxEl = document.getElementById('meteo-boxplot');
    if (boxEl) {
        const traces = ['rain', 'soilMoisture', 'tide'].map(id => ({
            y: extractValues(id, 3000), type: 'box',
            name: layerLabel(id), marker: { color: layerColor(id), size: 3 },
            boxpoints: 'outliers', jitter: 0.3,
        }));
        Plotly.newPlot(boxEl, traces, darkLayout('Box Plot — Rain vs Soil vs Tide', {
            height: 380, showlegend: false,
        }), window.PLOTLY_CFG);
    }

    // Scatter rain vs soil colored by flood
    const scEl = document.getElementById('meteo-scatter');
    if (scEl) {
        const indices = sampleIdx(1500);
        const rg = window.EDA.gridData.rain, sg = window.EDA.gridData.soilMoisture, lg = window.EDA.gridData.label;
        if (rg && sg) {
            const x = [], y = [], colors = [];
            for (const i of indices) {
                const rv = gridVal(rg, i), sv = gridVal(sg, i);
                if (rv === null || sv === null) continue;
                x.push(rv); y.push(sv);
                colors.push(lg && gridVal(lg, i) > 0 ? '#ef4444' : '#3b82f6');
            }
            Plotly.newPlot(scEl, [{
                x, y, mode: 'markers', type: 'scatter',
                marker: { color: colors, size: 4, opacity: 0.6 },
                hovertemplate: 'Rain: %{x:.2f}mm<br>Soil: %{y:.3f}<extra></extra>',
            }], darkLayout('Rain vs Soil Moisture (🔴 Flood / 🔵 Normal)', {
                height: 400,
                xaxis: { title: { text: 'Rain (mm)', font: { size: 10 } } },
                yaxis: { title: { text: 'Soil Moisture', font: { size: 10 } } },
            }), window.PLOTLY_CFG);
        }
    }
}

document.addEventListener('edaDataReady', renderMeteoPage);
