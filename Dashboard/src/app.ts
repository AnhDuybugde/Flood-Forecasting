import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import compression from 'compression';
import path from 'path';
import { S3Client, HeadObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { loadEnv } from './shared/config/env';
import { REGION_BOUNDS } from './shared/types/common';

import { pixelRouter } from './modules/pixel/pixel.controller';
import { metadataRouter } from './modules/metadata/metadata.controller';
import { gridRouter } from './modules/grid/grid.controller';
import { floodRiskRouter } from './modules/grid/flood-risk.controller';
import { forecastRouter } from './modules/forecast/forecast.controller';
import { inferenceRouter } from './modules/inference/inference.controller';
import { structuredLog } from './shared/middleware/tracing';

const app = express();

app.use(cors());
// Custom compression filter since default ignores binary octet-stream
app.use(compression({
    filter: (req, res) => {
        if (req.headers['x-no-compression']) return false;
        const type = res.getHeader('Content-Type') || '';
        if (typeof type === 'string' && type.includes('octet-stream')) return true;
        return compression.filter(req, res);
    }
}));
app.use(express.json());

// Request logging
app.use('/api', (req, res, next) => {
    structuredLog('info', 'api_request', { method: req.method, path: req.path });
    next();
});

// Proxy PNG Mask from Cloudflare R2 (CDN-optimized)
import { r2GetBuffer, tifKey, warmupTif } from './shared/legacy/r2-raster';
import { LAYER_FOLDER_MAP } from './shared/types/common';
import crypto from 'crypto';

app.get('/api/mask/:region/:date/label.png', async (req, res) => {
    const { region, date } = req.params;
    const r2Key = `FloodData/${region}/Mask/mask_${date}_label.png`;
    try {
        const buf = await r2GetBuffer(r2Key);
        const etag = `"${crypto.createHash('md5').update(buf).digest('hex')}"`;

        // Check If-None-Match for 304
        if (req.headers['if-none-match'] === etag) {
            return res.status(304).end();
        }

        res.setHeader('Content-Type', 'image/png');
        res.setHeader('Cache-Control', 'public, max-age=86400, stale-while-revalidate=3600');
        res.setHeader('ETag', etag);
        res.send(buf);
    } catch {
        res.status(404).send('Mask not found');
    }
});

// Proxy GeoTIFF directly to Frontend GeoRasterLayer (CDN-optimized)
app.get('/api/tif/:region/:layer/:date', async (req, res) => {
    const { region, layer, date } = req.params;
    const layerInfo = LAYER_FOLDER_MAP[layer as any];
    if (!layerInfo) return res.status(400).send('Invalid layer');

    const r2Key = tifKey(region, layerInfo, date);
    try {
        const buf = await r2GetBuffer(r2Key);
        const etag = `"${crypto.createHash('md5').update(buf).digest('hex')}"`;

        if (req.headers['if-none-match'] === etag) {
            return res.status(304).end();
        }

        res.setHeader('Content-Type', 'image/tiff');
        res.setHeader('Cache-Control', 'public, max-age=86400, stale-while-revalidate=3600');
        res.setHeader('ETag', etag);
        res.send(buf);
    } catch {
        res.status(404).send('TIF not found');
    }
});

// Warm-up GeoTIFF headers/IFDs to reduce first-click pixel latency (Range requests, no downsampling)
app.get('/api/warmup/:region/:date', async (req, res) => {
    const { region, date } = req.params;
    const r2KeyStacked = `FloodData/${region}/Stacked/stacked_${date}.tif`;
    await warmupTif(r2KeyStacked);
    // Fire-and-forget success (even if warmup fails, requests still work later)
    return res.json({ ok: true });
});
// Debug: verify R2 connectivity and sample key (no secrets in response)
app.get('/api/debug/r2-check', async (req, res) => {
    const env = loadEnv();
    const hasCreds = !!(env.R2_ACCOUNT_ID && env.R2_ACCESS_KEY_ID && env.R2_SECRET_ACCESS_KEY);
    if (!hasCreds) {
        return res.json({
            ok: false,
            message: 'R2 credentials not set (R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)',
            bucket: env.R2_BUCKET_NAME,
        });
    }
    const sampleKey = (req.query.key as string) || 'FloodData/DaNang/Stacked/stacked_2020-01-03.tif';
    try {
        const client = new S3Client({
            region: 'auto',
            endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
            credentials: {
                accessKeyId: env.R2_ACCESS_KEY_ID!,
                secretAccessKey: env.R2_SECRET_ACCESS_KEY!,
            },
        });
        await client.send(new HeadObjectCommand({ Bucket: env.R2_BUCKET_NAME, Key: sampleKey }));
        return res.json({ ok: true, message: 'R2 reachable', bucket: env.R2_BUCKET_NAME, sampleKey, exists: true });
    } catch (e: any) {
        const listPrefix = 'FloodData/DaNang/';
        try {
            const client = new S3Client({
                region: 'auto',
                endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
                credentials: {
                    accessKeyId: env.R2_ACCESS_KEY_ID!,
                    secretAccessKey: env.R2_SECRET_ACCESS_KEY!,
                },
            });
            const list = await client.send(new ListObjectsV2Command({
                Bucket: env.R2_BUCKET_NAME,
                Prefix: listPrefix,
                MaxKeys: 15,
            }));
            const keys = (list.Contents || []).map(o => o.Key).filter(Boolean);
            return res.json({
                ok: false,
                message: `Sample key not found: ${e.name || e.message}`,
                bucket: env.R2_BUCKET_NAME,
                sampleKey,
                exists: false,
                sampleKeysInBucket: keys,
            });
        } catch (listErr: any) {
            return res.json({
                ok: false,
                message: `R2 error: ${e.name || e.message}. List failed: ${listErr?.message || listErr}`,
                bucket: env.R2_BUCKET_NAME,
                sampleKey,
                exists: false,
            });
        }
    }
});

// Debug: pixel params + R2 key existence (no pixel read)
function normalizeDateForDebug(value: string): string | null {
    const s = String(value ?? '').trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    const m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (m) {
        const day = parseInt(m[1]!, 10), month = parseInt(m[2]!, 10), year = parseInt(m[3]!, 10);
        if (month >= 1 && month <= 12 && day >= 1 && day <= 31)
            return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    }
    return null;
}

app.get('/api/debug/pixel', async (req, res) => {
    const lat = Number(req.query.lat);
    const lng = Number(req.query.lng);
    const dateRaw = (req.query.date as string) ?? '';
    const region = (req.query.region as string) ?? 'DaNang';

    const normalizedDate = normalizeDateForDebug(dateRaw);
    if (!normalizedDate) {
        return res.json({
            ok: false,
            validation: 'invalid_date',
            message: 'date must be YYYY-MM-DD or DD/MM/YYYY',
            received: dateRaw,
        });
    }

    const bounds = REGION_BOUNDS[region];
    if (!bounds) {
        return res.json({
            ok: false,
            validation: 'unknown_region',
            region,
            normalizedDate,
        });
    }
    if (isNaN(lat) || isNaN(lng) || lat < bounds.south || lat > bounds.north || lng < bounds.west || lng > bounds.east) {
        return res.json({
            ok: false,
            validation: 'out_of_bounds',
            lat,
            lng,
            region,
            normalizedDate,
            bounds: { north: bounds.north, south: bounds.south, east: bounds.east, west: bounds.west },
        });
    }

    const r2KeyStacked = `FloodData/${region}/Stacked/stacked_${normalizedDate}.tif`;
    let keyExists = false;
    const env = loadEnv();
    if (env.R2_ACCOUNT_ID && env.R2_ACCESS_KEY_ID && env.R2_SECRET_ACCESS_KEY) {
        try {
            const client = new S3Client({
                region: 'auto',
                endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
                credentials: {
                    accessKeyId: env.R2_ACCESS_KEY_ID,
                    secretAccessKey: env.R2_SECRET_ACCESS_KEY,
                },
            });
            await client.send(new HeadObjectCommand({ Bucket: env.R2_BUCKET_NAME, Key: r2KeyStacked }));
            keyExists = true;
        } catch {
            keyExists = false;
        }
    }

    return res.json({
        ok: true,
        normalizedDate,
        region,
        r2KeyStacked,
        keyExists,
    });
});

// Debug: test NPZ reader — inspect zip contents
import { readPixelFromNpz } from './shared/legacy/npz-reader';
import AdmZip from 'adm-zip';
import { GetObjectCommand } from '@aws-sdk/client-s3';
app.get('/api/debug/npz', async (req, res) => {
    const lat = Number(req.query.lat) || 16.10;
    const lng = Number(req.query.lng) || 108.15;
    const date = (req.query.date as string) || '2020-01-03';
    const region = (req.query.region as string) || 'DaNang';

    const env = loadEnv();
    const key = `2020-2025/Data_Training_Soft_NPZ/Sample_${date}.npz`;

    const diag: Record<string, unknown> = {
        hasBounds: !!REGION_BOUNDS[region],
        hasR2Creds: !!(env.R2_ACCOUNT_ID && env.R2_ACCESS_KEY_ID && env.R2_SECRET_ACCESS_KEY),
        r2AccountId: env.R2_ACCOUNT_ID ? `${env.R2_ACCOUNT_ID.substring(0, 8)}...` : 'MISSING',
        bucket: env.R2_BUCKET_NAME,
        npzKey: key,
    };

    try {
        // Download raw NPZ to inspect
        const s3Client = new S3Client({
            region: 'auto',
            endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
            credentials: { accessKeyId: env.R2_ACCESS_KEY_ID!, secretAccessKey: env.R2_SECRET_ACCESS_KEY! },
        });
        const resp = await s3Client.send(new GetObjectCommand({ Bucket: env.R2_BUCKET_NAME, Key: key }));
        const arr = await resp.Body?.transformToByteArray();
        if (!arr) return res.json({ ok: false, error: 'Empty body' });

        const buf = Buffer.from(arr);
        diag.downloadSize = buf.length;

        const zip = new AdmZip(buf);
        const entries = zip.getEntries();
        diag.zipEntries = entries.map(e => ({
            name: e.entryName,
            size: e.header.size,
            compressedSize: e.header.compressedSize,
        }));

        // Inspect first entry's raw bytes
        for (const entry of entries) {
            const data = entry.getData();
            const first20 = Array.from(data.subarray(0, 20)).map(b => b.toString(16).padStart(2, '0'));
            const magic = data.toString('ascii', 0, 6);
            (diag as any)[`${entry.entryName}_first20hex`] = first20.join(' ');
            (diag as any)[`${entry.entryName}_magic`] = magic;
            (diag as any)[`${entry.entryName}_magic_codes`] = Array.from(data.subarray(0, 6)).map(b => b);
        }

        // Now try the actual pixel read
        const t0 = Date.now();
        const result = await readPixelFromNpz(region, date, lat, lng);
        const elapsed = Date.now() - t0;
        return res.json({ ok: true, elapsed, result, diag });
    } catch (e) {
        return res.json({ ok: false, error: (e as Error).message, stack: (e as Error).stack?.split('\n').slice(0, 5), diag });
    }
});

// ── Heatmap metadata (bounds + overlay URL for Leaflet imageOverlay) ──
app.get('/api/heatmap/:region/:date/:layer', async (req, res) => {
    const { region, date, layer } = req.params;
    const bounds = REGION_BOUNDS[region];
    if (!bounds) return res.status(400).json({ success: false, error: { code: 'VALIDATION', message: `Unknown region: ${region}` } });

    const validLayers = ['rain', 'soilMoisture', 'tide', 'label', 'flood', 'dem', 'slope', 'flow', 'landCover'];
    if (!validLayers.includes(layer!)) {
        return res.status(400).json({ success: false, error: { code: 'VALIDATION', message: `Invalid layer: ${layer}. Valid: ${validLayers.join(', ')}` } });
    }

    const effectiveLayer = layer === 'flood' ? 'label' : layer;
    const maskUrl = effectiveLayer === 'label'
        ? `/api/mask/${region}/${date}/label.png`
        : `/api/tif/${region}/${effectiveLayer}/${date}`;

    res.setHeader('Cache-Control', 'public, max-age=1800, stale-while-revalidate=600');
    return res.json({
        success: true,
        data: {
            layer,
            date,
            region,
            bounds: { north: bounds.north, south: bounds.south, east: bounds.east, west: bounds.west },
            maskUrl,
        },
    });
});

// ── Available layers check (probes local NPZ data for a given region + date) ──
app.get('/api/available-layers/:region/:date', async (req, res) => {
    const { region, date } = req.params;
    const bounds = REGION_BOUNDS[region];
    if (!bounds) return res.status(400).json({ success: false, error: { code: 'VALIDATION', message: `Unknown region: ${region}` } });

    if (!/^\d{4}-\d{2}-\d{2}$/.test(date!)) {
        return res.status(400).json({ success: false, error: { code: 'VALIDATION', message: 'Date must be YYYY-MM-DD' } });
    }

    const { loadNpzFromLocal } = await import('./shared/legacy/npz-reader');
    const npz = await loadNpzFromLocal(date!);

    const hasNpz = npz !== null;
    const layers = {
        rain: hasNpz && npz.bands >= 1,
        soilMoisture: hasNpz && npz.bands >= 2,
        tide: hasNpz && npz.bands >= 3,
        flood: hasNpz && npz.bands >= 4,
        static: hasNpz && npz.bands >= 5,
    };

    res.setHeader('Cache-Control', 'public, max-age=1800, stale-while-revalidate=600');
    return res.json({
        success: true,
        data: {
            region,
            date,
            layers,
            hasAnyData: hasNpz,
        },
    });
});

// Mount TS Routers
app.use('/api', metadataRouter);
app.use('/api', pixelRouter);
app.use('/api', gridRouter);
app.use('/api/v1', floodRiskRouter);
app.use('/api/forecast', forecastRouter);
app.use('/api/inference', inferenceRouter);

// Global Error Handler
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
    structuredLog('error', 'unhandled_error', { error: err.message, stack: err.stack });
    res.status(500).json({
        success: false,
        error: { code: 'INTERNAL', message: err.message }
    });
});

// 404 Handler
app.use('/api/*', (req, res) => {
    res.status(404).json({
        success: false,
        error: { code: 'NOT_FOUND', message: `Endpoint not found: ${req.path}` }
    });
});


// Serve frontend in local/render mode
const frontendPath = path.join(process.cwd(), 'frontend');
app.use(express.static(frontendPath, {
    setHeaders: (res, filePath) => {
        if (filePath.endsWith('.html') || filePath.endsWith('.js') || filePath.endsWith('.css')) {
            res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        } else {
            res.setHeader('Cache-Control', 'public, max-age=3600');
        }
    }
}));

app.get('*', (req, res) => {
    if (!req.path.startsWith('/api')) {
        res.sendFile(path.join(frontendPath, 'index.html'));
    }
});

export default app;
