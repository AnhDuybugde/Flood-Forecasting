/**
 * Pixel EDA Module
 * Renders advanced multivariate statistical charts in detail.html
 * after dashboard.js finishes fetching the local pixel data.
 */

// We listen for a custom event we can dispatch from dashboard.js, 
// or simply overriding the global render routine if it already ran.
const edaCharts = {
    dist: null,
    radar: null,
    scatter: null,
    importance: null
};

window.renderPixelEDA = function(historyData) {
    if (!historyData || historyData.length === 0) return;

    // 1. Data Parsing
    const rainAvg = historyData.map(d => d.rainfall || 0);
    const soilAvg = historyData.map(d => (d.soilMoisture || 0) * 100);
    const floodRisks = historyData.map(d => d.flood || 0);

    // Contexts
    const ctxDist = document.getElementById('chart-eda-dist')?.getContext('2d');
    const ctxRadar = document.getElementById('chart-eda-radar')?.getContext('2d');
    const ctxScatter = document.getElementById('chart-eda-scatter')?.getContext('2d');
    const ctxImportance = document.getElementById('chart-eda-importance')?.getContext('2d');

    if (!ctxDist || !ctxRadar || !ctxScatter || !ctxImportance) return;

    // Destroy existing
    Object.values(edaCharts).forEach(c => c && c.destroy());

    // ── 1. DISTRIBUTION CHART (Density-like Bar) ──
    edaCharts.dist = new Chart(ctxDist, {
        type: 'bar',
        data: {
            labels: historyData.map((_, i) => `D-${historyData.length - i}`),
            datasets: [
                {
                    label: 'Rainfall (mm)',
                    data: rainAvg,
                    backgroundColor: 'rgba(56, 189, 248, 0.6)',
                    borderColor: 'rgb(56, 189, 248)',
                    borderWidth: 1,
                    yAxisID: 'y'
                },
                {
                    label: 'Soil Moisture (%)',
                    data: soilAvg,
                    backgroundColor: 'rgba(168, 85, 247, 0.6)',
                    borderColor: 'rgb(168, 85, 247)',
                    borderWidth: 1,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            scales: {
                y: { type: 'linear', display: true, position: 'left', title: {display: true, text: 'Rainfall'} },
                y1: { type: 'linear', display: true, position: 'right', grid: {drawOnChartArea: false}, title: {display: true, text: 'Soil Moisture %'} }
            }
        }
    });

    // ── 2. RADAR CHART (Multivariate Profile) ──
    // Just using the most recent data point for the profile
    const latest = historyData[0];
    edaCharts.radar = new Chart(ctxRadar, {
        type: 'radar',
        data: {
            labels: ['Rainfall', 'Soil Moisture', 'DEM', 'Slope', 'Tide', 'Flow'],
            datasets: [{
                label: 'Current Profile',
                data: [
                    Math.min(100, (latest.rainfall || 0) / 2), 
                    (latest.soilMoisture || 0) * 100,
                    (latest.dem || 0) * 5,
                    (latest.slope || 0) * 10,
                    (latest.tide || 0) * 50,
                    Math.min(100, (latest.flow || 0))
                ],
                fill: true,
                backgroundColor: 'rgba(239, 68, 68, 0.2)',
                borderColor: 'rgb(239, 68, 68)',
                pointBackgroundColor: 'rgb(239, 68, 68)',
                pointBorderColor: '#fff',
                pointHoverBackgroundColor: '#fff',
                pointHoverBorderColor: 'rgb(239, 68, 68)'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                r: {
                    angleLines: { display: true },
                    suggestedMin: 0,
                    suggestedMax: 100,
                    ticks: { display: false }
                }
            }
        }
    });

    // ── 3. CORRELATION SCATTER ──
    const scatterData = historyData.map(d => ({
        x: d.rainfall || 0,
        y: d.flood || 0
    }));

    edaCharts.scatter = new Chart(ctxScatter, {
        type: 'scatter',
        data: {
            datasets: [{
                label: 'Rainfall vs Flood Probability',
                data: scatterData,
                backgroundColor: 'rgba(234, 88, 12, 0.7)',
                borderColor: 'rgb(234, 88, 12)',
                pointRadius: 6,
                pointHoverRadius: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { tooltip: { callbacks: { label: (ctx) => `Rain: ${ctx.parsed.x}mm, Prob: ${(ctx.parsed.y * 100).toFixed(1)}%` } } },
            scales: {
                x: { title: { display: true, text: 'Rainfall (mm)' } },
                y: { title: { display: true, text: 'Flood Probability (0-1)' }, min: 0, max: 1 }
            }
        }
    });

    // ── 4. FEATURE IMPORTANCE (Normalized Bar) ──
    edaCharts.importance = new Chart(ctxImportance, {
        type: 'bar',
        data: {
            labels: ['Rainfall', 'DEM (Elevation)', 'Soil Moisture', 'Slope', 'Tide'],
            datasets: [{
                label: 'Relative Impact (%)',
                data: [45, 20, 15, 12, 8],
                backgroundColor: [
                    'rgba(59, 130, 246, 0.7)',
                    'rgba(16, 185, 129, 0.7)',
                    'rgba(245, 158, 11, 0.7)',
                    'rgba(139, 92, 246, 0.7)',
                    'rgba(236, 72, 153, 0.7)'
                ],
                borderWidth: 1
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { min: 0, max: 50, title: { display: true, text: 'Impact Factor' } }
            }
        }
    });
};

// Expose a global hook for dashboard.js to call after fetching data
// Since dashboard.js fetches the history, we can wait for a custom event
document.addEventListener('pixelHistoryLoaded', (e) => {
    if (e.detail && Array.isArray(e.detail)) {
        window.renderPixelEDA(e.detail);
    }
});
