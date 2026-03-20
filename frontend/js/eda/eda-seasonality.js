/**
 * eda-seasonality.js — Rain Seasonality
 * Fetches all daily rain means from the backend and plots them grouped by year.
 */
'use strict';

window.EDA = window.EDA || {};

async function loadSeasonalityData() {
    const statusEl = document.getElementById('ssn-status');
    const chartEl = document.getElementById('ssn-line-chart');
    if (!statusEl || !chartEl) return;

    statusEl.innerHTML = '<span class="material-icons" style="font-size:14px;vertical-align:middle;animation:spin 1s linear infinite">autorenew</span> Đang tải... (có thể mất vài giây lần đầu)';
    
    try {
        const region = window.EDA.region || 'DaNang';
        const res = await fetch(`/api/forecast/${region}/seasonality`);
        if (!res.ok) throw new Error(`HTTP error ${res.status}`);
        
        const json = await res.json();
        if (!json.success) throw new Error(json.error?.message || 'Failed to fetch');
        
        const data = json.data;
        if (!data || !data.length) throw new Error('No data available');

        // Group data by year for both rain and tide
        const yearGroups = {};
        for (const record of data) {
            const y = record.year;
            if (!yearGroups[y]) yearGroups[y] = { x: [], rain: [], tide: [] };
            yearGroups[y].x.push(record.dayOfYear);
            yearGroups[y].rain.push(record.rain);
            yearGroups[y].tide.push(record.tide !== undefined ? record.tide : null);
        }

        // Prepare Plotly traces
        const rainTraces = [];
        const tideTraces = [];
        const years = Object.keys(yearGroups).sort();
        
        // Define colors per year
        const colorPalette = ['#38bdf8', '#34d399', '#f43f5e', '#fbbf24', '#a78bfa', '#fb923c', '#d946ef', '#2dd4bf'];
        
        for (let i = 0; i < years.length; i++) {
            const y = years[i];
            const color = colorPalette[i % colorPalette.length];
            
            // Rain Trace
            rainTraces.push({
                x: yearGroups[y].x,
                y: yearGroups[y].rain,
                mode: 'lines',
                name: y,
                line: { color: color, width: 2, shape: 'spline' },
                hovertemplate: `Day %{x}<br>Rain: %{y:.2f} mm<extra>${y}</extra>`
            });
            
            // Tide Trace
            tideTraces.push({
                x: yearGroups[y].x,
                y: yearGroups[y].tide,
                mode: 'lines',
                name: y,
                line: { color: color, width: 2, shape: 'spline' },
                hovertemplate: `Day %{x}<br>Tide: %{y:.2f} m<extra>${y}</extra>`
            });
        }

        const rainLayout = darkLayout('Rain Seasonality (mm) by Year', {
            height: 400,
            xaxis: { title: { text: 'Day of Year', font: { size: 12, color: '#94a3b8' } }, showgrid: true, gridcolor: 'rgba(51,65,85,0.4)' },
            yaxis: { title: { text: 'Rain (mm)', font: { size: 12, color: '#94a3b8' } }, showgrid: true, gridcolor: 'rgba(51,65,85,0.4)', rangemode: 'tozero' },
            legend: { orientation: 'h', y: -0.2, font: { color: '#e2e8f0' } },
            margin: { l: 60, r: 30, t: 30, b: 60 },
            hovermode: 'x unified'
        });

        const tideLayout = darkLayout('Tide Seasonality (m) by Year', {
            height: 400,
            xaxis: { title: { text: 'Day of Year', font: { size: 12, color: '#94a3b8' } }, showgrid: true, gridcolor: 'rgba(51,65,85,0.4)' },
            yaxis: { title: { text: 'Tide (m)', font: { size: 12, color: '#94a3b8' } }, showgrid: true, gridcolor: 'rgba(51,65,85,0.4)' },
            legend: { orientation: 'h', y: -0.2, font: { color: '#e2e8f0' } },
            margin: { l: 60, r: 30, t: 30, b: 60 },
            hovermode: 'x unified'
        });

        // Add CSS for spinning icon
        if (!document.getElementById('spin-anim')) {
            const style = document.createElement('style');
            style.id = 'spin-anim';
            style.textContent = '@keyframes spin { 100% { transform: rotate(360deg); } }';
            document.head.appendChild(style);
        }

        Plotly.newPlot(chartEl, rainTraces, rainLayout, PLOTLY_CFG);
        
        const tideChartEl = document.getElementById('tide-ssn-line-chart');
        if (tideChartEl) {
            Plotly.newPlot(tideChartEl, tideTraces, tideLayout, PLOTLY_CFG);
        }
        
        statusEl.innerHTML = `<span style="color:#10b981">✅ Tải xong (${data.length} ngày)</span>`;
        toast('✅ Đã tải dữ liệu Rain & Tide Seasonality', 'success');

    } catch (e) {
        console.error('Seasonality error:', e);
        statusEl.innerHTML = `<span style="color:#ef4444">❌ Lỗi: ${e.message}</span>`;
        toast('Lỗi tải dữ liệu mùa: ' + e.message, 'error');
    }
}

document.getElementById('btn-ssn-load')?.addEventListener('click', loadSeasonalityData);

