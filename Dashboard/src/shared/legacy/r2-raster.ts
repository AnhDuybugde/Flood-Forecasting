/**
 * R2 Raster Access Layer — COG Range Request Architecture
 *
 * KEY OPTIMIZATION: Uses GeoTIFF.fromUrl() with R2 public URL for HTTP Range requests.
 * Instead of downloading entire 50-500MB TIF files, only fetches the ~16KB tile
 * containing the requested pixel. This reduces latency from 5-15s to ~200ms.
 *
 * Fallback: If R2_PUBLIC_URL is not configured, falls back to full object download
 * via S3 SDK (legacy behavior).
 *
 * Cache strategy:
 *   - tifSourceCache: Caches GeoTIFF source objects (already parsed headers/IFDs)
 *   - pixelCache: Caches individual pixel results (20k entries, 1h TTL)
 */
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import * as GeoTIFF from 'geotiff';
import { loadEnv } from '../config/env';
import { MemoryCache } from '../cache/memory-cache';
import { structuredLog } from '../middleware/tracing';

const env = loadEnv();

// ── S3 Client (for JSON/buffer fetches and legacy fallback) ─────────
const r2 = new S3Client({
    region: 'auto',
    endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: env.R2_ACCESS_KEY_ID || '',
        secretAccessKey: env.R2_SECRET_ACCESS_KEY || '',
    },
});

// ── R2 Public URL for Range requests ────────────────────────────────
const R2_PUBLIC_BASE = env.R2_PUBLIC_URL
    ? env.R2_PUBLIC_URL.replace(/\/$/, '')
    : null;

// ── Caches ──────────────────────────────────────────────────────────
// GeoTIFF source objects (headers + IFD already parsed, supports Range reads)
const tifSourceCache = new MemoryCache<GeoTIFF.GeoTIFF>(200, 3600 * 1000);
// Presigned GET URLs (avoid regenerating per request)
// Keep slightly below presign expiry to avoid serving expired URLs.
const signedUrlCache = new MemoryCache<string>(500, 50 * 60 * 1000);
// Track warmups to avoid repeated work
const warmupCache = new MemoryCache<boolean>(500, 60 * 60 * 1000);
// Legacy: full GeoTIFFImage objects (fallback when R2_PUBLIC_URL not set)
const tifImageCache = new MemoryCache<GeoTIFF.GeoTIFFImage>(200, 3600 * 1000);
// Pixel results (L1 cache)
const pixelCache = new MemoryCache<number | null>(20_000, 3600 * 1000);

// ── Dedup: prevent duplicate in-flight requests for same key ────────
const pendingSourceOpens = new Map<string, Promise<GeoTIFF.GeoTIFF>>();
const pendingDownloads = new Map<string, Promise<GeoTIFF.GeoTIFFImage>>();
const pendingSignedUrls = new Map<string, Promise<string>>();

async function getPresignedUrl(r2Key: string): Promise<string> {
    const cached = signedUrlCache.get(r2Key);
    if (cached) return cached;

    if (pendingSignedUrls.has(r2Key)) return pendingSignedUrls.get(r2Key)!;

    const p = (async () => {
        const t0 = Date.now();
        try {
            const cmd = new GetObjectCommand({ Bucket: env.R2_BUCKET_NAME, Key: r2Key });
            const url = await getSignedUrl(r2, cmd, { expiresIn: 60 * 60 }); // 1h
            signedUrlCache.set(r2Key, url);
            structuredLog('info', 'r2_presign_url', { r2Key, durationMs: Date.now() - t0 });
            return url;
        } finally {
            pendingSignedUrls.delete(r2Key);
        }
    })();

    pendingSignedUrls.set(r2Key, p);
    return p;
}

// ═══════════════════════════════════════════════════════════════════
// JSON / Buffer helpers (unchanged)
// ═══════════════════════════════════════════════════════════════════

export async function r2GetJson(key: string): Promise<any> {
    const t0 = Date.now();
    try {
        const cmd = new GetObjectCommand({ Bucket: env.R2_BUCKET_NAME, Key: key });
        const res = await r2.send(cmd);
        const str = await res.Body?.transformToString();
        if (!str) return null;
        structuredLog('info', 'r2_get_json', { key, bytes: str.length, durationMs: Date.now() - t0 });
        return JSON.parse(str);
    } catch (e) {
        structuredLog('warn', 'r2_get_json_error', { key, durationMs: Date.now() - t0, error: (e as Error).message });
        return null;
    }
}

export async function r2GetBuffer(key: string): Promise<Buffer> {
    const t0 = Date.now();
    const cmd = new GetObjectCommand({ Bucket: env.R2_BUCKET_NAME, Key: key });
    const res = await r2.send(cmd);
    const arr = await res.Body?.transformToByteArray();
    if (!arr) throw new Error("Empty body");
    const buf = Buffer.from(arr.buffer, arr.byteOffset, arr.byteLength);
    structuredLog('info', 'r2_get_buffer', { key, bytes: buf.byteLength, durationMs: Date.now() - t0 });
    return buf;
}

export async function loadDateIndex(region: string): Promise<any> {
    return r2GetJson(`FloodData/${region}/metadata.json`);
}

export function tifKey(region: string, layerInfo: any, date: string): string {
    if (layerInfo.isFlat) {
        if (layerInfo.folder) {
            return `FloodData/${region}/${layerInfo.sub}/${layerInfo.folder}/${layerInfo.prefix}.tif`;
        }
        return `FloodData/${region}/${layerInfo.sub}/${layerInfo.prefix}.tif`;
    }
    if (layerInfo.folder) {
        return `FloodData/${region}/${layerInfo.sub}/${layerInfo.folder}/${layerInfo.prefix}_${date}.tif`;
    }
    return `FloodData/${region}/${layerInfo.sub}/${layerInfo.prefix}_${date}.tif`;
}

// ═══════════════════════════════════════════════════════════════════
// COG Range Request: Open GeoTIFF source via HTTP URL
// Only fetches headers + IFD (~4-16KB). Tile data fetched lazily on readRasters().
// ═══════════════════════════════════════════════════════════════════

async function getCachedTifSource(r2Key: string): Promise<GeoTIFF.GeoTIFF> {
    const cached = tifSourceCache.get(r2Key);
    if (cached) return cached;

    if (pendingSourceOpens.has(r2Key)) {
        return pendingSourceOpens.get(r2Key)!;
    }

    const openPromise = (async () => {
        try {
            let url: string;
            let urlMode: 'public' | 'presigned';
            if (R2_PUBLIC_BASE) {
                url = `${R2_PUBLIC_BASE}/${r2Key}`;
                urlMode = 'public';
            } else {
                url = await getPresignedUrl(r2Key);
                urlMode = 'presigned';
            }

            const tiff = await GeoTIFF.fromUrl(url, {
                allowFullFile: false,   // Never download entire file
            });
            tifSourceCache.set(r2Key, tiff);
            structuredLog('info', 'tif_source_open', { r2Key, mode: urlMode });
            return tiff;
        } catch (e) {
            structuredLog('error', 'tif_source_open_error', { r2Key, error: (e as Error).message });
            throw e;
        } finally {
            pendingSourceOpens.delete(r2Key);
        }
    })();

    pendingSourceOpens.set(r2Key, openPromise);
    return openPromise;
}

// Warm-up: open GeoTIFF source + image metadata ahead of time (no pixel downsampling)
export async function warmupTif(r2Key: string): Promise<void> {
    if (warmupCache.get(r2Key)) return;
    const t0 = Date.now();
    try {
        const tiff = await getCachedTifSource(r2Key);
        // Force image metadata (IFD) load; data tiles still lazy.
        await tiff.getImage();
        warmupCache.set(r2Key, true);
        structuredLog('info', 'tif_warmup_ok', { r2Key, durationMs: Date.now() - t0 });
    } catch (e) {
        structuredLog('warn', 'tif_warmup_err', { r2Key, durationMs: Date.now() - t0, error: (e as Error).message });
    }
}

// ═══════════════════════════════════════════════════════════════════
// Legacy fallback: Full object download (when R2_PUBLIC_URL not set)
// ═══════════════════════════════════════════════════════════════════

async function getCachedTifImageLegacy(r2Key: string): Promise<GeoTIFF.GeoTIFFImage> {
    const cached = tifImageCache.get(r2Key);
    if (cached) return cached;

    if (pendingDownloads.has(r2Key)) {
        return pendingDownloads.get(r2Key)!;
    }

    const downloadPromise = (async () => {
        const t0 = Date.now();
        try {
            const cmd = new GetObjectCommand({ Bucket: env.R2_BUCKET_NAME, Key: r2Key });
            const res = await r2.send(cmd);
            const arr = await res.Body?.transformToByteArray();
            if (!arr) throw new Error("Empty body");

            const slice = arr.buffer.slice(arr.byteOffset, arr.byteOffset + arr.byteLength) as ArrayBuffer;
            const tiff = await GeoTIFF.fromArrayBuffer(slice);
            const img = await tiff.getImage();
            tifImageCache.set(r2Key, img);
            structuredLog('info', 'tif_full_download', { r2Key, bytes: arr.byteLength, durationMs: Date.now() - t0 });
            return img;
        } catch (e) {
            structuredLog('error', 'tif_download_error', { r2Key, error: (e as Error).message });
            throw e;
        } finally {
            pendingDownloads.delete(r2Key);
        }
    })();

    pendingDownloads.set(r2Key, downloadPromise);
    return downloadPromise;
}

// ═══════════════════════════════════════════════════════════════════
// Public API: getCachedTifImage
// Tries R2 public URL (Range request) first; on 404/network error falls back to S3 GetObject.
// ═══════════════════════════════════════════════════════════════════

export async function getCachedTifImage(r2Key: string): Promise<GeoTIFF.GeoTIFFImage> {
    // Prefer range-request path (public or presigned). Fallback to legacy full download only if it fails.
    try {
        const tiff = await getCachedTifSource(r2Key);
        return tiff.getImage();
    } catch (e) {
        structuredLog('warn', 'tif_range_fallback', {
            r2Key,
            error: (e as Error).message,
            fallback: 'full_download',
        });
        return getCachedTifImageLegacy(r2Key);
    }
}

// ═══════════════════════════════════════════════════════════════════
// readPixelFromR2Tif — Optimized pixel reading
// With COG Range requests, readRasters() only fetches the ~16KB tile
// containing the target pixel, NOT the entire file.
// ═══════════════════════════════════════════════════════════════════

export async function readPixelFromR2Tif(r2Key: string, lat: number, lng: number, scale = 1): Promise<number | null> {
    const cacheKey = `pixel_${r2Key}_${lat.toFixed(4)}_${lng.toFixed(4)}`;
    const cached = pixelCache.get(cacheKey);
    if (cached !== undefined) return cached;

    try {
        const img = await getCachedTifImage(r2Key);
        const bbox = img.getBoundingBox();
        const west = bbox[0] ?? 0, south = bbox[1] ?? 0, east = bbox[2] ?? 0, north = bbox[3] ?? 0;

        const width = img.getWidth();
        const height = img.getHeight();

        const col = Math.floor((lng - west) / (east - west) * width);
        const row = Math.floor((north - lat) / (north - south) * height);

        if (col < 0 || col >= width || row < 0 || row >= height) {
            pixelCache.set(cacheKey, null);
            return null;
        }

        const rasterData = await img.readRasters({ window: [col, row, col + 1, row + 1] });
        const band = rasterData[0] as Float64Array | null;
        const rawValue = band ? band[0] : null;
        const nodata = (img.fileDirectory as any).GDAL_NODATA;
        const nod = nodata !== undefined ? parseFloat(nodata) : -9999;

        const value = (rawValue === null || rawValue === undefined || rawValue === nod || isNaN(rawValue) || rawValue <= -9998)
            ? null
            : parseFloat((rawValue / scale).toFixed(4));

        pixelCache.set(cacheKey, value);
        return value;
    } catch (e) {
        return null;
    }
}

// ═══════════════════════════════════════════════════════════════════
// Multi-band pixel reading for stacked COGs (Range request optimized)
// Reads all 8 bands from a single pixel window in one Range request
// ═══════════════════════════════════════════════════════════════════

export async function readStackedPixel(
    r2Key: string, lat: number, lng: number,
    bandNames: readonly string[], bandScales: readonly number[],
): Promise<Record<string, number | null> | null> {
    const cacheKey = `stk_${r2Key}_${lat.toFixed(4)}_${lng.toFixed(4)}`;
    const cached = pixelCache.get(cacheKey as any);
    if (cached !== undefined) return cached as any;

    try {
        const img = await getCachedTifImage(r2Key);
        const bbox = img.getBoundingBox();
        const west = bbox[0] ?? 0, south = bbox[1] ?? 0, east = bbox[2] ?? 0, north = bbox[3] ?? 0;
        const width = img.getWidth();
        const height = img.getHeight();

        const col = Math.floor((lng - west) / (east - west) * width);
        const row = Math.floor((north - lat) / (north - south) * height);

        if (col < 0 || col >= width || row < 0 || row >= height) return null;

        const rasterData = await img.readRasters({ window: [col, row, col + 1, row + 1] });
        const nodataStr = (img.fileDirectory as any).GDAL_NODATA;
        const nod = nodataStr !== undefined ? parseFloat(nodataStr) : -9999;

        const result: Record<string, number | null> = {};
        for (let i = 0; i < bandNames.length; i++) {
            const bandName = bandNames[i];
            const scale = bandScales[i];
            if (!bandName || scale === undefined) continue;
            const band = rasterData[i];
            const raw = band ? (band as Float64Array)[0] : null;
            if (raw === null || raw === undefined || raw === nod || isNaN(raw) || raw <= -9998) {
                result[bandName] = null;
            } else {
                result[bandName] = parseFloat((raw / scale).toFixed(4));
            }
        }

        // Cache as any to reuse pixelCache (stores compound result keyed differently)
        (pixelCache as any).set(cacheKey, result);
        return result;
    } catch {
        return null;
    }
}
