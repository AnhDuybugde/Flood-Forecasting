/**
 * Application Entry Point
 * 100% TypeScript, No api/ directory legacy
 */
import path from 'path';
import dotenv from 'dotenv';

// Load .env from project root (folder containing package.json). When run from dist/, __dirname is dist/ so .env is ../.env
const projectRoot = path.resolve(__dirname, '..');
dotenv.config({ path: path.join(projectRoot, '.env') });

import app from './app';
import { loadEnv } from './shared/config/env';
import { structuredLog } from './shared/middleware/tracing';
import { TifWorkerPool } from './shared/worker/pool';

// Inject Dependencies for Legacy Fallback & TS Services
import { injectDeps as injectPixelDeps } from './modules/pixel/pixel.service';
import { injectDeps as injectMetadataDeps } from './modules/metadata/metadata.service';
import { injectDeps as injectGridDeps } from './modules/grid/grid.service';
import * as legacyR2 from './shared/legacy/r2-raster';

const env = loadEnv();

structuredLog('info', 'ts_bootstrap', {
    nodeEnv: env.NODE_ENV,
    port: env.PORT,
    useWorkerPool: env.USE_WORKER_POOL,
    usePrebuiltGrid: env.USE_PREBUILT_GRID,
    // Range request is supported via:
    // - Public URL (fastest, no signing)
    // - Presigned URL fallback (automatic, still supports HTTP Range)
    r2Mode: env.R2_PUBLIC_URL ? 'range_request_public' : 'range_request_presigned',
});

if (!env.R2_PUBLIC_URL) {
    structuredLog('info', 'r2_public_url_missing', {
        hint: 'R2_PUBLIC_URL is optional. Backend will use presigned URLs for HTTP Range requests automatically.',
        recommendation: 'Set R2_PUBLIC_URL for best performance (public URL avoids signing and can reduce overhead).',
    });
}

// ── Dependency Injection (Ngắt kết nối api.js cũ) ─────────────────────────
injectPixelDeps({
    getCachedTifImage: legacyR2.getCachedTifImage,
    readPixelFromR2Tif: legacyR2.readPixelFromR2Tif,
    tifKey: legacyR2.tifKey as any,
    readStackedPixel: legacyR2.readStackedPixel,
});

injectMetadataDeps({
    loadDateIndex: legacyR2.loadDateIndex,
    r2GetJson: legacyR2.r2GetJson
});

injectGridDeps({
    r2GetJson: legacyR2.r2GetJson,
    r2GetBuffer: legacyR2.r2GetBuffer,
    tifKey: legacyR2.tifKey,
    getCachedTifImage: legacyR2.getCachedTifImage,
});

// ── Worker Pool (Tối ưu giải nén TIFF) ──────────────────────────────────────
let workerPool: TifWorkerPool | null = null;
if (env.USE_WORKER_POOL) {
    workerPool = new TifWorkerPool({
        maxWorkers: env.WORKER_POOL_SIZE,
        maxQueueSize: 50,
        taskTimeoutMs: 5000,
    });
    structuredLog('info', 'worker_pool', { status: 'ready', size: env.WORKER_POOL_SIZE });
}
export function getWorkerPool() { return workerPool; }


// ── Start Express Server ───────────────────────────────────────────────────
const server = app.listen(env.PORT, () => {
    console.log(`🚀 Vietnam Flood Dashboard running at http://localhost:${env.PORT}`);
    console.log(`📊 Press Ctrl+C to stop the server`);
});

// ── Graceful Shutdown ──────────────────────────────────────────────────────
const shutdown = async (signal: string) => {
    structuredLog('info', 'shutdown', { reason: signal });
    server.close();
    if (workerPool) await workerPool.shutdown();
    process.exit(0);
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
