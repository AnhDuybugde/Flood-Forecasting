/**
 * eda-advanced.js — Nâng Cao: 5 charts
 * Q-Q Plot, PCA Biplot, t-SNE Projection, Dendrogram, Clustered Correlation Heatmap
 */
'use strict';

function renderAdvancedPage() {
    const G = window.EDA.gridData;
    const numL = window.LAYERS.filter(l => !l.isCat).map(l => l.id);

    // Populate Q-Q select
    const qqSel = document.getElementById('qq-layer-select');
    if (qqSel) {
        qqSel.innerHTML = '';
        window.LAYERS.forEach(l => { const o = document.createElement('option'); o.value = l.id; o.textContent = l.label; qqSel.appendChild(o); });
    }

    // 1. Q-Q Plot
    function renderQQ(lid) {
        const el = document.getElementById('chart-qq'); if (!el) return;
        const vals = extractValues(lid, 0).sort((a, b) => a - b);
        if (vals.length < 20) return;
        const n = vals.length;
        // Normal quantiles via inverse CDF approximation
        function qnorm(p) {
            if (p <= 0) return -4; if (p >= 1) return 4;
            const t = Math.sqrt(-2 * Math.log(p < 0.5 ? p : 1 - p));
            const c = [2.515517, 0.802853, 0.010328];
            const d = [1.432788, 0.189269, 0.001308];
            let q = t - (c[0] + c[1] * t + c[2] * t * t) / (1 + d[0] * t + d[1] * t * t + d[2] * t * t * t);
            return p < 0.5 ? -q : q;
        }
        const step = Math.max(1, Math.floor(n / 500));
        const theoretical = [], sample = [];
        for (let i = 0; i < n; i += step) {
            const p = (i + 0.5) / n;
            theoretical.push(qnorm(p));
            sample.push(vals[i]);
        }
        const min = Math.min(...theoretical), max = Math.max(...theoretical);
        const sMean = sample.reduce((a, b) => a + b, 0) / sample.length;
        let sStd = 0; sample.forEach(v => sStd += (v - sMean) ** 2); sStd = Math.sqrt(sStd / sample.length) || 1;
        const refLine = [sMean + min * sStd, sMean + max * sStd];

        Plotly.newPlot(el, [
            { x: theoretical, y: sample, type: 'scatter', mode: 'markers', marker: { color: layerColor(lid), size: 4, opacity: 0.6 }, name: 'Data', hovertemplate: 'Theoretical: %{x:.2f}<br>Sample: %{y:.3f}<extra></extra>' },
            { x: [min, max], y: refLine, type: 'scatter', mode: 'lines', line: { color: '#ef4444', dash: 'dash', width: 2 }, name: 'Normal Ref', showlegend: true }
        ], darkLayout(`Q-Q Plot — ${layerLabel(lid)} vs Normal`, { height: 400, xaxis: { title: { text: 'Theoretical Quantiles', font: { size: 10 } } }, yaxis: { title: { text: 'Sample Quantiles', font: { size: 10 } } }, legend: { font: { color: '#94a3b8' } } }), PLOTLY_CFG);
    }
    renderQQ(numL[0] || window.LAYERS[0]?.id);
    qqSel?.addEventListener('change', e => renderQQ(e.target.value));

    // 2. PCA Biplot (simple 2D via covariance eigen-decomposition approx)
    const pcaEl = document.getElementById('chart-pca');
    if (pcaEl && numL.length >= 2) {
        const N = 800, indices = sampleIdx(N);
        const data = indices.map(i => numL.map(id => { const g = G[id]; return g ? (gridVal(g, i) || 0) : 0; }));
        const d = numL.length;
        // Standardize
        const means = Array(d).fill(0), stds = Array(d).fill(0);
        data.forEach(row => row.forEach((v, j) => means[j] += v));
        means.forEach((_, j) => means[j] /= data.length);
        data.forEach(row => row.forEach((v, j) => stds[j] += (v - means[j]) ** 2));
        stds.forEach((_, j) => stds[j] = Math.sqrt(stds[j] / data.length) || 1);
        const std = data.map(row => row.map((v, j) => (v - means[j]) / stds[j]));
        // Covariance
        const cov = Array.from({ length: d }, () => Array(d).fill(0));
        std.forEach(row => { for (let i = 0; i < d; i++) for (let j = 0; j < d; j++) cov[i][j] += row[i] * row[j]; });
        cov.forEach(row => row.forEach((_, j, r) => r[j] /= data.length));
        // Power iteration for top 2 eigenvectors
        function powerIter(mat, n, prev) {
            let v = prev || Array.from({ length: n }, () => Math.random());
            for (let iter = 0; iter < 50; iter++) {
                const nv = Array(n).fill(0);
                for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) nv[i] += mat[i][j] * v[j];
                const norm = Math.sqrt(nv.reduce((s, x) => s + x * x, 0)) || 1;
                v = nv.map(x => x / norm);
            }
            return v;
        }
        const pc1 = powerIter(cov, d);
        // Deflate
        const cov2 = cov.map((row, i) => row.map((v, j) => v - pc1[i] * pc1[j] * cov.flat().reduce((s, x) => s + Math.abs(x), 0) / d));
        const pc2 = powerIter(cov2, d);
        // Project
        const x = std.map(row => row.reduce((s, v, j) => s + v * pc1[j], 0));
        const y = std.map(row => row.reduce((s, v, j) => s + v * pc2[j], 0));
        const colors = indices.map(i => G.label && gridVal(G.label, i) > 0 ? '#ef4444' : '#3b82f6');

        Plotly.newPlot(pcaEl, [
            { x, y, type: 'scatter', mode: 'markers', marker: { color: colors, size: 4, opacity: 0.6 }, name: 'Data', hovertemplate: 'PC1: %{x:.2f}<br>PC2: %{y:.2f}<extra></extra>' },
            // Loading vectors
            ...numL.map((id, j) => ({
                x: [0, pc1[j] * 3], y: [0, pc2[j] * 3], type: 'scatter', mode: 'lines+text',
                line: { color: layerColor(id), width: 2 }, text: ['', layerLabel(id)],
                textposition: 'top center', textfont: { color: layerColor(id), size: 9 },
                showlegend: false, hoverinfo: 'skip'
            }))
        ], darkLayout('PCA Biplot (PC1 vs PC2)', { height: 450, xaxis: { title: { text: 'PC1', font: { size: 10 } } }, yaxis: { title: { text: 'PC2', font: { size: 10 } } } }), PLOTLY_CFG);
    }

    // 3. t-SNE (simplified: gradient descent on pairwise distances)
    const tsneEl = document.getElementById('chart-tsne');
    if (tsneEl && numL.length >= 2) {
        const N = 400, indices = sampleIdx(N);
        const data = indices.map(i => numL.map(id => { const g = G[id]; return g ? (gridVal(g, i) || 0) : 0; }));
        // Standardize
        const d = numL.length, means = Array(d).fill(0), stds = Array(d).fill(0);
        data.forEach(row => row.forEach((v, j) => means[j] += v));
        means.forEach((_, j) => means[j] /= data.length);
        data.forEach(row => row.forEach((v, j) => stds[j] += (v - means[j]) ** 2));
        stds.forEach((_, j) => stds[j] = Math.sqrt(stds[j] / data.length) || 1);
        const std = data.map(row => row.map((v, j) => (v - means[j]) / stds[j]));

        // Simple MDS-like projection using Sammon mapping approximation
        const n = std.length;
        // Distance matrix (high-dim)
        const distH = Array.from({ length: n }, () => Array(n).fill(0));
        for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
            let s = 0; for (let k = 0; k < d; k++) s += (std[i][k] - std[j][k]) ** 2;
            distH[i][j] = distH[j][i] = Math.sqrt(s);
        }
        // Initialize 2D randomly, then optimize
        const pos = Array.from({ length: n }, () => [Math.random() * 2 - 1, Math.random() * 2 - 1]);
        const lr = 0.5;
        for (let iter = 0; iter < 100; iter++) {
            for (let i = 0; i < n; i++) {
                let gx = 0, gy = 0;
                for (let j = 0; j < n; j++) {
                    if (i === j) continue;
                    const dx = pos[i][0] - pos[j][0], dy = pos[i][1] - pos[j][1];
                    const dL = Math.sqrt(dx * dx + dy * dy) || 0.001;
                    const dH = distH[i][j] || 0.001;
                    const factor = (dL - dH) / (dH * dL);
                    gx += factor * dx; gy += factor * dy;
                }
                pos[i][0] -= lr * gx / n; pos[i][1] -= lr * gy / n;
            }
        }
        const colors = indices.map(i => G.label && gridVal(G.label, i) > 0 ? '#ef4444' : '#3b82f6');
        Plotly.newPlot(tsneEl, [{ x: pos.map(p => p[0]), y: pos.map(p => p[1]), type: 'scatter', mode: 'markers', marker: { color: colors, size: 5, opacity: 0.7 }, hovertemplate: 'Dim1: %{x:.2f}<br>Dim2: %{y:.2f}<extra></extra>' }], darkLayout('t-SNE-like Projection (🔴 Flood / 🔵 Normal)', { height: 420, xaxis: { title: { text: 'Dim 1', font: { size: 10 } } }, yaxis: { title: { text: 'Dim 2', font: { size: 10 } } } }), PLOTLY_CFG);
    }

    // 4. Dendrogram (hierarchical clustering of layers)
    const dendEl = document.getElementById('chart-dendrogram');
    if (dendEl && numL.length >= 2) {
        // Distance = 1 - |pearson|
        const n = numL.length, dist = Array.from({ length: n }, () => Array(n).fill(0));
        for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
            const gA = G[numL[i]], gB = G[numL[j]];
            if (!gA || !gB) { dist[i][j] = dist[j][i] = 1; continue; }
            const dA = gA.data, dB = gB.data, ndA = gA.nodata ?? -9999, ndB = gB.nodata ?? -9999;
            const sA = gA.scale || 1, sB = gB.scale || 1;
            let sa = 0, sb = 0, sab = 0, sa2 = 0, sb2 = 0, c = 0;
            const step = Math.max(1, Math.floor(dA.length / 3000));
            for (let k = 0; k < dA.length; k += step) { if (dA[k] === ndA || dA[k] <= -9998 || dB[k] === ndB || dB[k] <= -9998) continue; const a = dA[k] / sA, b = dB[k] / sB; sa += a; sb += b; sab += a * b; sa2 += a * a; sb2 += b * b; c++; }
            const num = c * sab - sa * sb, den = Math.sqrt((c * sa2 - sa * sa) * (c * sb2 - sb * sb));
            dist[i][j] = dist[j][i] = 1 - Math.abs(den ? num / den : 0);
        }
        // Agglomerative clustering (single linkage)
        const clusters = numL.map((_, i) => [i]);
        const merges = [];
        const activeDist = dist.map(r => [...r]);
        for (let m = 0; m < n - 1; m++) {
            let minD = Infinity, mi = -1, mj = -1;
            for (let i = 0; i < clusters.length; i++) for (let j = i + 1; j < clusters.length; j++) {
                let d = Infinity;
                for (const ci of clusters[i]) for (const cj of clusters[j]) d = Math.min(d, dist[ci][cj]);
                if (d < minD) { minD = d; mi = i; mj = j; }
            }
            merges.push({ left: mi, right: mj, dist: minD, label: clusters[mi].map(i => layerLabel(numL[i])).join('+') + ' & ' + clusters[mj].map(i => layerLabel(numL[i])).join('+') });
            clusters[mi] = [...clusters[mi], ...clusters[mj]];
            clusters.splice(mj, 1);
        }
        // Plot as horizontal bar chart showing merge distances
        const mLabels = merges.map((m, i) => `Merge ${i + 1}`);
        const mDists = merges.map(m => m.dist);
        const mTexts = merges.map(m => m.label);
        Plotly.newPlot(dendEl, [{ y: mLabels, x: mDists, type: 'bar', orientation: 'h', marker: { color: mDists.map(d => `hsl(${(1 - d) * 240}, 70%, 55%)`), opacity: 0.85 }, text: mTexts, hoverinfo: 'text+x', textposition: 'outside', textfont: { color: '#94a3b8', size: 9 } }], darkLayout('Dendrogram — Layer Merging Distance', { height: Math.max(280, merges.length * 50 + 80), margin: { l: 80, r: 180, t: 44, b: 40 }, xaxis: { title: { text: '1 - |Pearson r|', font: { size: 10 } } } }), PLOTLY_CFG);
    }

    // 5. Clustered Correlation Heatmap (reorder by clustering)
    const chEl = document.getElementById('chart-cluster-heatmap');
    if (chEl && numL.length >= 2) {
        // Cluster order via simple sort by mean correlation
        const corrM = numL.map(id1 => numL.map(id2 => {
            if (id1 === id2) return 1;
            const gA = G[id1], gB = G[id2]; if (!gA || !gB) return 0;
            const dA = gA.data, dB = gB.data, ndA = gA.nodata ?? -9999, ndB = gB.nodata ?? -9999, sA = gA.scale || 1, sB = gB.scale || 1;
            let sa = 0, sb = 0, sab = 0, sa2 = 0, sb2 = 0, c = 0;
            const step = Math.max(1, Math.floor(dA.length / 5000));
            for (let k = 0; k < dA.length; k += step) { if (dA[k] === ndA || dA[k] <= -9998 || dB[k] === ndB || dB[k] <= -9998) continue; const a = dA[k] / sA, b = dB[k] / sB; sa += a; sb += b; sab += a * b; sa2 += a * a; sb2 += b * b; c++; }
            const num = c * sab - sa * sb, den = Math.sqrt((c * sa2 - sa * sa) * (c * sb2 - sb * sb));
            return den ? num / den : 0;
        }));
        // Sort order by mean abs correlation
        const order = numL.map((_, i) => ({ i, mean: corrM[i].reduce((s, v) => s + Math.abs(v), 0) / numL.length })).sort((a, b) => b.mean - a.mean).map(o => o.i);
        const labels = order.map(i => layerLabel(numL[i]));
        const zOrdered = order.map(i => order.map(j => corrM[i][j]));
        const annots = [];
        for (let i = 0; i < labels.length; i++) for (let j = 0; j < labels.length; j++) annots.push({ x: labels[j], y: labels[i], text: zOrdered[i][j].toFixed(2), showarrow: false, font: { size: 9, color: Math.abs(zOrdered[i][j]) > 0.5 ? '#fff' : '#e2e8f0' } });
        Plotly.newPlot(chEl, [{ z: zOrdered, x: labels, y: labels, type: 'heatmap', colorscale: 'RdBu', reversescale: true, zmin: -1, zmax: 1, colorbar: { thickness: 12, tickfont: { color: '#94a3b8' } } }], darkLayout('Clustered Correlation Heatmap', { height: 450, annotations: annots, xaxis: { tickangle: -30 } }), PLOTLY_CFG);
    }
}

document.addEventListener('edaDataReady', renderAdvancedPage);
