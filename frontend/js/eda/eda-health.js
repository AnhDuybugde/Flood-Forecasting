/**
 * eda-health.js — Channel Health Monitor
 * Auto-detect empty channels, zero ratios, NaN counts, outlier percentages.
 * Renders a health dashboard with quality badges on the sidebar.
 */
'use strict';

function renderHealthPage() {
    const G = window.EDA.gridData;
    if (!Object.keys(G).length) return;

    const container = document.getElementById('health-cards');
    if (!container) return;
    container.innerHTML = '';

    const healthData = [];

    window.LAYERS.forEach(layer => {
        const grid = G[layer.id];
        if (!grid) {
            healthData.push({ id: layer.id, label: layer.label, color: layer.color, status: 'missing', validPct: 0, zeroPct: 100, nanCount: 0, min: 0, max: 0, mean: 0 });
            return;
        }

        const d = grid.data, nd = grid.nodata ?? -9999, sc = grid.scale || 1;
        let valid = 0, zeros = 0, nans = 0, min = Infinity, max = -Infinity, sum = 0;
        const total = d.length;

        for (let i = 0; i < total; i++) {
            const raw = d[i];
            if (raw === nd || raw <= -9998 || raw == null || isNaN(raw)) { nans++; continue; }
            const v = raw / sc;
            valid++;
            sum += v;
            if (v === 0) zeros++;
            if (v < min) min = v;
            if (v > max) max = v;
        }

        const mean = valid > 0 ? sum / valid : 0;
        const validPct = (valid / total * 100);
        const zeroPct = valid > 0 ? (zeros / valid * 100) : 100;
        const isEmpty = valid === 0 || (zeroPct === 100);

        // Outlier detection (IQR-based, sampled)
        let outlierPct = 0;
        if (valid > 100) {
            const samples = [];
            const step = Math.max(1, Math.floor(total / 5000));
            for (let i = 0; i < total; i += step) {
                const raw = d[i];
                if (raw === nd || raw <= -9998 || raw == null || isNaN(raw)) continue;
                samples.push(raw / sc);
            }
            samples.sort((a, b) => a - b);
            const q1 = samples[Math.floor(samples.length * 0.25)];
            const q3 = samples[Math.floor(samples.length * 0.75)];
            const iqr = q3 - q1;
            const lo = q1 - 1.5 * iqr, hi = q3 + 1.5 * iqr;
            const outliers = samples.filter(v => v < lo || v > hi).length;
            outlierPct = (outliers / samples.length * 100);
        }

        let status = 'good';
        if (isEmpty) status = 'empty';
        else if (validPct < 50) status = 'critical';
        else if (zeroPct > 90) status = 'warning';
        else if (outlierPct > 10) status = 'warning';

        healthData.push({ id: layer.id, label: layer.label, color: layer.color, icon: layer.icon, status, validPct, zeroPct, nanCount: nans, outlierPct, min: min === Infinity ? 0 : min, max: max === -Infinity ? 0 : max, mean, total });
    });

    // Update sidebar badges
    healthData.forEach(h => {
        const badge = document.querySelector(`.health-badge-${h.id}`);
        if (badge) {
            const colors = { good: '#10b981', warning: '#f59e0b', critical: '#ef4444', empty: '#64748b', missing: '#64748b' };
            const icons = { good: '✓', warning: '⚠', critical: '✗', empty: '∅', missing: '?' };
            badge.style.background = colors[h.status] + '33';
            badge.style.color = colors[h.status];
            badge.textContent = icons[h.status];
            badge.title = `${h.label}: ${h.status}`;
        }
    });

    // Summary cards
    const good = healthData.filter(h => h.status === 'good').length;
    const warn = healthData.filter(h => h.status === 'warning').length;
    const crit = healthData.filter(h => h.status === 'critical' || h.status === 'empty' || h.status === 'missing').length;

    let summaryHtml = `
    <div class="grid-3" style="margin-bottom:20px">
        <div class="card" style="text-align:center;padding:20px">
            <div style="font-size:32px;font-weight:800;color:#10b981">${good}</div>
            <div style="font-size:12px;color:var(--text-dim);margin-top:4px">Channels OK ✓</div>
        </div>
        <div class="card" style="text-align:center;padding:20px">
            <div style="font-size:32px;font-weight:800;color:#f59e0b">${warn}</div>
            <div style="font-size:12px;color:var(--text-dim);margin-top:4px">Warnings ⚠</div>
        </div>
        <div class="card" style="text-align:center;padding:20px">
            <div style="font-size:32px;font-weight:800;color:#ef4444">${crit}</div>
            <div style="font-size:12px;color:var(--text-dim);margin-top:4px">Critical / Empty ✗</div>
        </div>
    </div>`;
    container.innerHTML = summaryHtml;

    // Per-channel detail cards
    healthData.forEach(h => {
        const statusColors = { good: '#10b981', warning: '#f59e0b', critical: '#ef4444', empty: '#64748b', missing: '#64748b' };
        const statusLabels = { good: 'OK', warning: 'Warning', critical: 'Critical', empty: 'Empty', missing: 'Missing' };
        const sc = statusColors[h.status], sl = statusLabels[h.status];

        const card = document.createElement('div');
        card.className = 'card';
        card.style.marginBottom = '12px';
        card.innerHTML = `
        <div class="card-hdr">
            <div class="card-title">
                <span class="material-icons" style="color:${h.color}">${h.icon || 'layers'}</span>
                ${h.label}
                <span style="display:inline-flex;align-items:center;gap:4px;margin-left:8px;padding:2px 10px;border-radius:20px;font-size:10px;font-weight:700;background:${sc}22;color:${sc};border:1px solid ${sc}44">
                    ${sl}
                </span>
            </div>
        </div>
        <div class="card-body" style="padding:16px">
            <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:12px">
                <div style="background:rgba(30,41,59,0.5);border-radius:8px;padding:10px 14px">
                    <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase;font-weight:600">Valid Pixels</div>
                    <div style="font-size:18px;font-weight:700;color:${h.validPct > 90 ? '#10b981' : h.validPct > 50 ? '#f59e0b' : '#ef4444'}">${h.validPct.toFixed(1)}%</div>
                </div>
                <div style="background:rgba(30,41,59,0.5);border-radius:8px;padding:10px 14px">
                    <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase;font-weight:600">Zero Ratio</div>
                    <div style="font-size:18px;font-weight:700;color:${h.zeroPct < 50 ? '#10b981' : h.zeroPct < 90 ? '#f59e0b' : '#ef4444'}">${h.zeroPct.toFixed(1)}%</div>
                </div>
                <div style="background:rgba(30,41,59,0.5);border-radius:8px;padding:10px 14px">
                    <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase;font-weight:600">NaN / NoData</div>
                    <div style="font-size:18px;font-weight:700;color:var(--text)">${h.nanCount.toLocaleString()}</div>
                </div>
                <div style="background:rgba(30,41,59,0.5);border-radius:8px;padding:10px 14px">
                    <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase;font-weight:600">Outliers (IQR)</div>
                    <div style="font-size:18px;font-weight:700;color:${(h.outlierPct||0) < 5 ? '#10b981' : '#f59e0b'}">${(h.outlierPct||0).toFixed(1)}%</div>
                </div>
                <div style="background:rgba(30,41,59,0.5);border-radius:8px;padding:10px 14px">
                    <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase;font-weight:600">Min</div>
                    <div style="font-size:16px;font-weight:700;color:var(--text);font-family:monospace">${h.min.toFixed(4)}</div>
                </div>
                <div style="background:rgba(30,41,59,0.5);border-radius:8px;padding:10px 14px">
                    <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase;font-weight:600">Max</div>
                    <div style="font-size:16px;font-weight:700;color:var(--text);font-family:monospace">${h.max.toFixed(4)}</div>
                </div>
                <div style="background:rgba(30,41,59,0.5);border-radius:8px;padding:10px 14px">
                    <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase;font-weight:600">Mean</div>
                    <div style="font-size:16px;font-weight:700;color:var(--text);font-family:monospace">${h.mean.toFixed(4)}</div>
                </div>
            </div>
            <div id="health-bar-${h.id}" style="margin-top:12px"></div>
        </div>`;
        container.appendChild(card);
    });

    // Draw mini bar charts for valid/zero/nan composition
    healthData.forEach(h => {
        if (h.status === 'missing') return;
        const validR = h.validPct, zeroR = h.zeroPct * (validR / 100), nanR = 100 - validR;
        Plotly.newPlot(`health-bar-${h.id}`, [{
            x: ['Valid Non-Zero', 'Valid Zero', 'NaN/NoData'],
            y: [validR - zeroR, zeroR, nanR],
            type: 'bar',
            marker: { color: ['#10b981', '#f59e0b', '#ef4444'], opacity: 0.8 },
            text: [(validR - zeroR).toFixed(1) + '%', zeroR.toFixed(1) + '%', nanR.toFixed(1) + '%'],
            textposition: 'outside',
            textfont: { size: 10, color: '#94a3b8' }
        }], darkLayout('', { height: 180, margin: { l: 40, r: 20, t: 10, b: 50 }, yaxis: { title: { text: '%', font: { size: 10 } }, range: [0, 110] } }), PLOTLY_CFG);
    });

    // Overall quality radar chart
    const radarLabels = healthData.map(h => h.label);
    const radarValues = healthData.map(h => {
        let score = 0;
        if (h.status !== 'missing') {
            score += Math.min(h.validPct, 100) * 0.4;
            score += Math.max(0, 100 - h.zeroPct) * 0.3;
            score += Math.max(0, 100 - (h.outlierPct || 0)) * 0.3;
        }
        return Math.round(score);
    });
    Plotly.newPlot('health-radar', [{
        type: 'scatterpolar',
        r: [...radarValues, radarValues[0]],
        theta: [...radarLabels, radarLabels[0]],
        fill: 'toself',
        fillcolor: 'rgba(99,102,241,0.15)',
        line: { color: '#6366f1', width: 2 },
        marker: { color: healthData.map(h => h.color), size: 8 },
        name: 'Quality Score'
    }], darkLayout('Data Quality Radar — per Channel', {
        height: 420,
        polar: {
            bgcolor: 'rgba(15,23,42,0.3)',
            radialaxis: { visible: true, range: [0, 100], tickfont: { size: 9, color: '#64748b' }, gridcolor: 'rgba(51,65,85,0.4)' },
            angularaxis: { tickfont: { size: 10, color: '#94a3b8' }, gridcolor: 'rgba(51,65,85,0.3)' }
        }
    }), PLOTLY_CFG);
}

document.addEventListener('edaDataReady', renderHealthPage);
