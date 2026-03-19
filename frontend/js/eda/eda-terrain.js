/**
 * eda-terrain.js — Sub-page 3: Địa Hình — DEM, Slope, Flow
 */
'use strict';

function terrainHeatmap(layerId, divId, cscale) {
    const grid = window.EDA.gridData[layerId];
    if (!grid) return;
    const rows = grid.size.r, cols = grid.size.c, ds = 6;
    const dr = Math.ceil(rows / ds), dc = Math.ceil(cols / ds);
    const z = [];
    for (let r = 0; r < dr; r++) {
        const row = [];
        for (let c = 0; c < dc; c++) row.push(gridVal(grid, Math.min(r * ds, rows - 1) * cols + Math.min(c * ds, cols - 1)));
        z.push(row);
    }
    const b = grid.bounds;
    Plotly.newPlot(divId, [{
        z, type: 'heatmap', x0: b.w, dx: (b.e - b.w) / dc, y0: b.s, dy: (b.n - b.s) / dr,
        colorscale: cscale, zsmooth: 'best',
        colorbar: { thickness: 10, tickfont: { size: 8, color: '#94a3b8' } },
        hovertemplate: `%{z:.3f} ${layerUnit(layerId)}<extra>${layerLabel(layerId)}</extra>`,
    }], darkLayout('', { height: 280, margin: { l: 45, r: 10, t: 10, b: 35 } }), window.PLOTLY_CFG);
}

function render3DSurface() {
    const el = document.getElementById('terrain-3d');
    if (!el) return;
    const grid = window.EDA.gridData.dem;
    if (!grid) { el.innerHTML = '<p style="color:var(--text-muted);text-align:center;padding:40px">nodata</p>'; return; }

    const rows = grid.size.r, cols = grid.size.c, ds = 12;
    const dr = Math.ceil(rows / ds), dc = Math.ceil(cols / ds);
    const z = [];
    for (let r = 0; r < dr; r++) {
        const row = [];
        for (let c = 0; c < dc; c++) row.push(gridVal(grid, Math.min(r * ds, rows - 1) * cols + Math.min(c * ds, cols - 1)) || 0);
        z.push(row);
    }

    Plotly.newPlot(el, [{
        z, type: 'surface',
        colorscale: 'Earth',
        lighting: { ambient: 0.6, diffuse: 0.7, specular: 0.15, roughness: 0.5 },
        contours: { z: { show: true, usecolormap: true, project: { z: true } } },
        hovertemplate: 'DEM: %{z:.1f}m<extra></extra>',
    }], darkLayout('DEM 3D Surface', {
        height: 450,
        scene: {
            xaxis: { title: '', gridcolor: 'rgba(51,65,85,0.3)', color: '#94a3b8' },
            yaxis: { title: '', gridcolor: 'rgba(51,65,85,0.3)', color: '#94a3b8' },
            zaxis: { title: 'DEM (m)', gridcolor: 'rgba(51,65,85,0.3)', color: '#94a3b8' },
            bgcolor: 'rgba(0,0,0,0)',
            camera: { eye: { x: 1.5, y: 1.5, z: 1.2 } },
        },
        margin: { l: 10, r: 10, t: 44, b: 10 },
    }), window.PLOTLY_CFG);
}

function renderTerrainScatter() {
    const el = document.getElementById('terrain-scatter');
    if (!el) return;
    const dg = window.EDA.gridData.dem, sg = window.EDA.gridData.slope;
    if (!dg || !sg) return;
    const indices = sampleIdx(1500);
    const x = [], y = [], colors = [];
    const fg = window.EDA.gridData.flow;
    for (const i of indices) {
        const dv = gridVal(dg, i), sv = gridVal(sg, i);
        if (dv === null || sv === null) continue;
        x.push(dv); y.push(sv);
        const fv = fg ? (gridVal(fg, i) || 0) : 0;
        colors.push(Math.log1p(Math.abs(fv)));
    }
    Plotly.newPlot(el, [{
        x, y, mode: 'markers', type: 'scatter',
        marker: { color: colors, colorscale: 'Viridis', size: 4, opacity: 0.6,
            colorbar: { title: { text: 'Log(Flow)', font: { size: 9, color: '#94a3b8' } }, thickness: 10, tickfont: { size: 8, color: '#94a3b8' } } },
        hovertemplate: 'DEM: %{x:.1f}m<br>Slope: %{y:.2f}°<extra></extra>',
    }], darkLayout('DEM vs Slope (color = log Flow)', {
        height: 400,
        xaxis: { title: { text: 'DEM (m)', font: { size: 10 } } },
        yaxis: { title: { text: 'Slope (°)', font: { size: 10 } } },
    }), window.PLOTLY_CFG);
}

function renderTerrainPage() {
    terrainHeatmap('dem', 'terrain-dem-heatmap', 'Earth');
    terrainHeatmap('slope', 'terrain-slope-heatmap', 'Hot');
    terrainHeatmap('flow', 'terrain-flow-heatmap', 'Purples');
    render3DSurface();
    renderTerrainScatter();
}

document.addEventListener('edaDataReady', renderTerrainPage);
