/**
 * Grid Service — Serves pre-built grid JSON or generates from TIF/NPZ.
 *
 * Strategy priority:
 *   1. In-memory cache (fastest)
 *   2. Pre-built JSON from R2 (zero decode)
 *   3. On-the-fly TIF decode from R2 (COG range request)
 *   4. Local NPZ file decode (offline fallback)
 */
import type { Result } from '../../shared/types/result';
import type { AppError } from '../../shared/types/result';
import { Ok, Err, AppErrors } from '../../shared/types/result';
import { MemoryCache } from '../../shared/cache/memory-cache';
import { structuredLog } from '../../shared/middleware/tracing';
import type { GridParams, GridJSON } from './grid.types';
import { getEnv } from '../../shared/config/env';
import { LAYER_FOLDER_MAP, REGION_BOUNDS, STACKED_BAND_NAMES } from '../../shared/types/common';
import fs from 'fs';
import path from 'path';
import { promises as fsp } from 'fs';

const gridCache = new MemoryCache<GridJSON>(100, 30 * 60 * 1000);

// Layer name → NPZ band index (matches STACKED_BAND_NAMES order)
const LAYER_TO_BAND_INDEX: Record<string, number> = {
    rain: 0, soilMoisture: 1, tide: 2, label: 3,
    dem: 4, slope: 5, flow: 6, landCover: 7,
};

let tifFailCount = 0;
const TIF_FAIL_THRESHOLD = 3;

function denormalizeNpz(bandName: string, raw: number): number {
    switch (bandName) {
        case 'rainfall':     return Math.round(raw * 200 * 100) / 100;
        case 'soilMoisture': return Math.round(raw * 0.5 * 10000) / 10000;
        case 'tide':         return Math.round((raw * 3.0 - 1.5) * 1000) / 1000;
        case 'slope':        return Math.round(raw * 90 * 100) / 100;
        case 'flow':         return raw > 0 ? Math.round((Math.pow(10, raw * 5) - 1) * 100) / 100 : 0;
        case 'dem':          return Math.round(raw * 10000) / 10000;
        case 'landCover':    return Math.round(raw * 100000) / 100000;
        case 'flood':        return raw >= 0.5 ? 1 : 0;
        default:             return raw;
    }
}

// ── Dependencies ───────────────────────────────────────────
let _r2GetBuffer: ((key: string) => Promise<Buffer>) | null = null;
let _r2GetJson: ((key: string) => Promise<any>) | null = null;
let _tifKey: ((region: string, layerInfo: any, date: string) => string) | null = null;
let _getCachedTifImage: ((r2Key: string) => Promise<any>) | null = null;

export function injectDeps(deps: {
    r2GetJson?: (key: string) => Promise<any>;
    r2GetBuffer?: (key: string) => Promise<Buffer>;
    tifKey?: (region: string, layerInfo: any, date: string) => string;
    getCachedTifImage?: (r2Key: string) => Promise<any>;
}) {
    _r2GetJson = deps.r2GetJson ?? null;
    _r2GetBuffer = deps.r2GetBuffer ?? null;
    _tifKey = deps.tifKey ?? null;
    _getCachedTifImage = deps.getCachedTifImage ?? null;
}

const CATEGORICAL_LAYERS = new Set(['label', 'landCover']);

export function downsampleGrid(grid: GridJSON, maxRows: number): GridJSON {
    const { r: srcRows, c: srcCols } = grid.size;
    if (srcRows <= maxRows) return grid;

    const ratio = maxRows / srcRows;
    const dstRows = maxRows;
    const dstCols = Math.max(1, Math.round(srcCols * ratio));

    const rowStep = srcRows / dstRows;
    const colStep = srcCols / dstCols;
    const data = new Float32Array(dstRows * dstCols);
    const isCat = CATEGORICAL_LAYERS.has(grid.layer);

    for (let dr = 0; dr < dstRows; dr++) {
        for (let dc = 0; dc < dstCols; dc++) {
            if (isCat) {
                const srcR = Math.min(Math.floor((dr + 0.5) * rowStep), srcRows - 1);
                const srcC = Math.min(Math.floor((dc + 0.5) * colStep), srcCols - 1);
                data[dr * dstCols + dc] = grid.data[srcR * srcCols + srcC]!;
            } else {
                const srcR0 = Math.floor(dr * rowStep);
                const srcR1 = Math.min(Math.floor((dr + 1) * rowStep), srcRows);
                const srcC0 = Math.floor(dc * colStep);
                const srcC1 = Math.min(Math.floor((dc + 1) * colStep), srcCols);

                let sum = 0, count = 0;
                for (let r = srcR0; r < srcR1; r++) {
                    for (let c = srcC0; c < srcC1; c++) {
                        const v = grid.data[r * srcCols + c]!;
                        if (v !== grid.nodata && v > -9998 && !isNaN(v)) {
                            sum += v;
                            count++;
                        }
                    }
                }
                data[dr * dstCols + dc] = count > 0
                    ? Math.round((sum / count) * 10000) / 10000
                    : grid.nodata;
            }
        }
    }

    return { ...grid, size: { r: dstRows, c: dstCols }, data };
}

export async function getGrid(params: GridParams): Promise<Result<GridJSON, AppError>> {
    const { region, date, layer } = params;
    const t0 = Date.now();
    const env = getEnv();
    const cacheKey = `grid_${region}_${date}_${layer}`;

    // ── Strategy 0: In-memory cache ──
    const cached = gridCache.get(cacheKey);
    if (cached) {
        structuredLog('info', 'grid_cache_hit', { region, date, layer, durationMs: Date.now() - t0 });
        return Ok(cached);
    }

    // ── Strategy 0.5: Pre-built .gridbin file from Cloudflare R2 ──
    if (env.USE_PREBUILT_GRID && _r2GetBuffer) {
        try {
            const r2BinKey = `grid-bin/grid_${date}_${layer}.gridbin`;
            // Local disk cache to avoid repeated downloads on slow networks
            const cacheDir = path.resolve(process.cwd(), '.cache', 'grid-bin');
            const cachePath = path.join(cacheDir, `grid_${date}_${layer}.gridbin`);

            let buf: Buffer | null = null;
            try {
                buf = await fsp.readFile(cachePath);
                structuredLog('info', 'grid_prebuilt_disk_bin', { region, date, layer, durationMs: Date.now() - t0 });
            } catch {
                // ignore cache miss
            }
            if (!buf) {
                buf = await _r2GetBuffer(r2BinKey);
                // best-effort persist
                fsp.mkdir(cacheDir, { recursive: true })
                    .then(() => fsp.writeFile(cachePath, buf!))
                    .catch(() => { /* ignore */ });
            }

            const metaLen = buf.readUInt32LE(0);
            const meta = JSON.parse(buf.toString('utf-8', 4, 4 + metaLen));
            const data = new Float32Array(buf.buffer, buf.byteOffset + 4 + metaLen, (buf.byteLength - 4 - metaLen) / 4);
            const gridJson: GridJSON = { ...meta, data };
            
            gridCache.set(cacheKey, gridJson);
            structuredLog('info', 'grid_prebuilt_r2_bin', { region, date, layer, durationMs: Date.now() - t0 });
            return Ok(gridJson);
        } catch (err) {
            structuredLog('warn', 'grid_prebuilt_r2_bin_error', { region, date, layer, error: (err as Error).message });
        }
    }

    // ── Strategy 1: Pre-built JSON from R2 ──
    if (env.USE_PREBUILT_GRID && _r2GetJson) {
        try {
            const r2Key = `FloodData/${region}/Grid/grid_${date}_${layer}.json`;
            const gridJson = await _r2GetJson(r2Key) as GridJSON;
            if (gridJson && gridJson.v) {
                gridCache.set(cacheKey, gridJson);
                structuredLog('info', 'grid_prebuilt', { region, date, layer, durationMs: Date.now() - t0 });
                return Ok(gridJson);
            }
        } catch {
            // Pre-built not available — fall through to TIF decode
        }
    }

    // ── Validate layer & region (shared by Strategy 2 + 3) ──
    const layerInfo = LAYER_FOLDER_MAP[layer];
    if (!layerInfo) {
        return Err(AppErrors.validation(`Unknown layer: ${layer}`));
    }
    const bounds = REGION_BOUNDS[region];
    if (!bounds) {
        return Err(AppErrors.validation(`Unknown region: ${region}`));
    }

    // ── Strategy 2: On-the-fly decode from TIF (with 8s timeout + circuit breaker) ──
    if (_tifKey && _getCachedTifImage && tifFailCount < TIF_FAIL_THRESHOLD) {
    try {
        const r2Key = _tifKey(region, layerInfo, date);
        const img = await Promise.race([
            _getCachedTifImage(r2Key),
            new Promise<never>((_, reject) => setTimeout(() => reject(new Error('TIF fetch timeout (8s)')), 8000)),
        ]);

        const width = img.getWidth();
        const height = img.getHeight();
        const rasterData = await img.readRasters();
        const band = rasterData[0] as Float64Array;

        if (!band || band.length === 0) {
            return Err(AppErrors.notFound(`No raster data for ${layer} on ${date}`));
        }

        const nodataStr = (img.fileDirectory as any).GDAL_NODATA;
        const nod = nodataStr !== undefined ? parseFloat(nodataStr) : -9999;
        const bbox = img.getBoundingBox();

        const total = height * width;
        const data = new Float32Array(total);
        for (let i = 0; i < total; i++) {
            const raw = band[i]!;
            if (raw === null || raw === undefined || raw === nod || isNaN(raw) || raw <= -9998) {
                data[i] = -9999;
            } else {
                data[i] = Math.round(raw * 100) / 100;
            }
        }

        const gridJson: GridJSON = {
            v: 1,
            region,
            date,
            layer,
            bounds: { n: bbox[3], s: bbox[1], e: bbox[2], w: bbox[0] },
            size: { r: height, c: width },
            scale: layerInfo.scale,
            nodata: -9999,
            data,
        };

        gridCache.set(cacheKey, gridJson);
        tifFailCount = 0;
        structuredLog('info', 'grid_tif_decode', {
            region, date, layer,
            rasterSize: `${width}x${height}`,
            durationMs: Date.now() - t0,
        });
        return Ok(gridJson);
    } catch (err) {
        tifFailCount++;
        structuredLog('warn', 'grid_tif_error', { region, date, layer, failCount: tifFailCount, error: (err as Error).message });
    }
    } // end if (_tifKey && _getCachedTifImage)

    // ── Strategy 3: Local NPZ file decode (and R2 fallback) ──
    try {
        const { loadNpzFromLocal, loadNpzFromR2 } = await import('../../shared/legacy/npz-reader');
        let npz = await loadNpzFromLocal(date);
        if (!npz) {
            npz = await loadNpzFromR2(date);
        }
        if (npz) {
            const bandIdx = LAYER_TO_BAND_INDEX[layer];
            if (bandIdx === undefined || bandIdx >= npz.bands) {
                return Err(AppErrors.validation(`Layer "${layer}" not available in NPZ (bands=${npz.bands})`));
            }

            const bandName = STACKED_BAND_NAMES[bandIdx];
            const { x, height, width } = npz;
            const total = height * width;
            const bandOffset = bandIdx * total;

            const data = new Float32Array(total);
            for (let i = 0; i < total; i++) {
                const raw = x[bandOffset + i]!;
                if (raw === undefined || isNaN(raw) || raw < 0) {
                    data[i] = -9999;
                } else {
                    data[i] = bandName ? denormalizeNpz(bandName, raw) : raw;
                }
            }

            const regionBounds = REGION_BOUNDS[region]!;
            const gridJson: GridJSON = {
                v: 1,
                region, date, layer,
                bounds: {
                    n: regionBounds.north, s: regionBounds.south,
                    e: regionBounds.east, w: regionBounds.west,
                },
                size: { r: height, c: width },
                scale: 1,
                nodata: -9999,
                data,
            };

            gridCache.set(cacheKey, gridJson);
            structuredLog('info', 'grid_npz_decode', {
                region, date, layer, bandIdx,
                rasterSize: `${width}x${height}`,
                durationMs: Date.now() - t0,
            });
            return Ok(gridJson);
        }
    } catch (err) {
        structuredLog('error', 'grid_npz_error', { region, date, layer, error: (err as Error).message });
    }

    return Err(AppErrors.notFound(`Grid data not available for ${layer} on ${date}`));
}
