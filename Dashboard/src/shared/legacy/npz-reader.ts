/**
 * NPZ Reader — Parse numpy .npz files from R2 for pixel data.
 *
 * NPZ = zip archive containing .npy files.
 * .npy = magic + header (shape, dtype) + raw binary data.
 *
 * This module downloads NPZ from R2, parses the 'x' array (8 bands)
 * and 'y' array (flood label), caches the result, and provides
 * pixel lookup by lat/lng using region bounds.
 */
import AdmZip from 'adm-zip';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { loadEnv } from '../config/env';
import { MemoryCache } from '../cache/memory-cache';
import { structuredLog } from '../middleware/tracing';
import { REGION_BOUNDS, STACKED_BAND_NAMES } from '../types/common';

// NPZ R2 key pattern
const NPZ_PREFIX = 'training/Data_Training_Soft_NPZ';
function npzKey(date: string): string {
    return `${NPZ_PREFIX}/Sample_${date}.npz`;
}

interface ParsedNpz {
    x: Float32Array; // flattened (8 * H * W)
    y: Float32Array; // flattened (H * W)
    bands: number;
    height: number;
    width: number;
}

// Cache parsed NPZ data (each ~25MB raw, ~32MB parsed)
// Keep max 5 dates in memory (~160MB)
const npzCache = new MemoryCache<ParsedNpz>(5, 30 * 60 * 1000);
const pendingLoads = new Map<string, Promise<ParsedNpz | null>>();

/**
 * Parse a .npy buffer into typed array + shape.
 * Format: \x93NUMPY + version(2B) + header_len(2B or 4B) + header_str + raw_data
 */
function parseNpy(buf: Buffer): { data: Float32Array; shape: number[] } {
    // Magic: first byte is 0x93 (above ASCII range), followed by 'NUMPY'
    if (buf[0] !== 0x93 || buf.toString('ascii', 1, 6) !== 'NUMPY') {
        throw new Error('Not a valid .npy file');
    }

    const major = buf[6]!;
    let headerLen: number;
    let headerOffset: number;

    if (major === 1) {
        headerLen = buf.readUInt16LE(8);
        headerOffset = 10;
    } else {
        headerLen = buf.readUInt32LE(8);
        headerOffset = 12;
    }

    const headerStr = buf.toString('ascii', headerOffset, headerOffset + headerLen);

    // Parse shape from header like "{'descr': '<f4', 'fortran_order': False, 'shape': (8, 1115, 1856), }"
    const shapeMatch = headerStr.match(/shape['"]\s*:\s*\(([^)]+)\)/);
    if (!shapeMatch) throw new Error(`Cannot parse shape from npy header: ${headerStr}`);
    const shape = shapeMatch[1]!.split(',').map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n));

    const descrMatch = headerStr.match(/descr['"]\s*:\s*'([^']+)'/);
    const descr = descrMatch ? descrMatch[1]! : '<f4';

    const dataOffset = headerOffset + headerLen;
    const totalElements = shape.reduce((a, b) => a * b, 1);

    let data: Float32Array;
    if (descr === '<f4' || descr === 'float32') {
        data = new Float32Array(buf.buffer, buf.byteOffset + dataOffset, totalElements);
    } else if (descr === '<f8' || descr === 'float64') {
        const f64 = new Float64Array(buf.buffer, buf.byteOffset + dataOffset, totalElements);
        data = new Float32Array(f64);
    } else {
        // Best effort: treat as float32
        data = new Float32Array(buf.buffer, buf.byteOffset + dataOffset, totalElements);
    }

    return { data, shape };
}

/**
 * Load and parse NPZ from local filesystem (fallback when R2 is unreachable).
 */
import fs from 'fs';
import path from 'path';

const LOCAL_NPZ_DIR = path.resolve(process.cwd(), 'data', 'training', 'Data_Training_Soft_NPZ');

export async function loadNpzFromLocal(date: string): Promise<ParsedNpz | null> {
    const cacheKey = `npz_${date}`;
    const cached = npzCache.get(cacheKey);
    if (cached) return cached;

    const filePath = path.join(LOCAL_NPZ_DIR, `Sample_${date}.npz`);
    if (!fs.existsSync(filePath)) return null;

    try {
        const t0 = Date.now();
        const buf = fs.readFileSync(filePath);
        const zip = new AdmZip(buf);
        const entries = zip.getEntries();

        let xData: Float32Array | null = null;
        let yData: Float32Array | null = null;
        let xShape: number[] = [];

        for (const entry of entries) {
            if (entry.entryName === 'x.npy') {
                const parsed = parseNpy(entry.getData());
                xData = parsed.data;
                xShape = parsed.shape;
            } else if (entry.entryName === 'y.npy') {
                const parsed = parseNpy(entry.getData());
                yData = parsed.data;
            }
        }

        if (!xData || xShape.length !== 3) return null;

        const [bands, height, width] = xShape as [number, number, number];
        const result: ParsedNpz = {
            x: xData,
            y: yData || new Float32Array(height * width),
            bands, height, width,
        };

        npzCache.set(cacheKey, result);
        structuredLog('info', 'npz_local_loaded', { date, durationMs: Date.now() - t0, bands, height, width });
        return result;
    } catch (e) {
        structuredLog('error', 'npz_local_error', { date, error: (e as Error).message });
        return null;
    }
}

/**
 * List available dates from local NPZ files.
 */
export function listLocalNpzDates(): string[] {
    if (!fs.existsSync(LOCAL_NPZ_DIR)) return [];
    const files = fs.readdirSync(LOCAL_NPZ_DIR);
    return files
        .filter(f => f.startsWith('Sample_') && f.endsWith('.npz'))
        .map(f => f.replace('Sample_', '').replace('.npz', ''))
        .filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d))
        .sort();
}

/**
 * Compute region-average rainfall (mm) from local NPZ for the trend chart.
 * Band 0 = rainfall — auto-detects whether normalized (0-1) or physical (mm).
 * Samples a 20×20 grid and returns the mean rainfall in mm.
 */
export async function getLocalRainfallTotal(date: string, region: string): Promise<number | null> {
    const npz = await loadNpzFromLocal(date);
    if (!npz) return null;

    const { x, bands, height, width } = npz;
    if (bands < 1) return null;

    const bounds = REGION_BOUNDS[region];
    const gridRows = bounds?.rows ?? 20;
    const gridCols = bounds?.cols ?? 20;

    const samples: number[] = [];
    for (let gr = 0; gr < gridRows; gr++) {
        for (let gc = 0; gc < gridCols; gc++) {
            const r = Math.floor((gr + 0.5) / gridRows * height);
            const c = Math.floor((gc + 0.5) / gridCols * width);
            const idx = 0 * height * width + r * width + c;
            const val = x[idx];
            if (val !== undefined && !isNaN(val) && val >= 0) {
                samples.push(val);
            }
        }
    }

    if (samples.length === 0) return 0;

    const rawMean = samples.reduce((a, b) => a + b, 0) / samples.length;
    const maxSample = Math.max(...samples);

    // Auto-detect normalization: if max <= 1.5, data is in [0,1] range → denorm × 200
    const mean = maxSample <= 1.5 ? rawMean * 200 : rawMean;

    return parseFloat(mean.toFixed(2));
}

/**
 * Download and parse NPZ from R2.
 */
export async function loadNpzFromR2(date: string): Promise<ParsedNpz | null> {
    const key = npzKey(date);
    const cacheKey = `npz_${date}`;

    const cached = npzCache.get(cacheKey);
    if (cached) return cached;

    if (pendingLoads.has(cacheKey)) {
        return pendingLoads.get(cacheKey)!;
    }

    const loadPromise = (async (): Promise<ParsedNpz | null> => {
        const env = loadEnv();
        if (!env.R2_ACCOUNT_ID || !env.R2_ACCESS_KEY_ID || !env.R2_SECRET_ACCESS_KEY) {
            structuredLog('error', 'npz_no_creds', { key });
            return null;
        }

        const s3 = new S3Client({
            region: 'auto',
            endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
            credentials: {
                accessKeyId: env.R2_ACCESS_KEY_ID,
                secretAccessKey: env.R2_SECRET_ACCESS_KEY,
            },
        });

        try {
            structuredLog('info', 'npz_download_start', { key });
            const t0 = Date.now();
            const resp = await s3.send(new GetObjectCommand({
                Bucket: env.R2_BUCKET_NAME,
                Key: key,
            }));

            const arr = await resp.Body?.transformToByteArray();
            if (!arr) {
                structuredLog('error', 'npz_empty_body', { key });
                return null;
            }

            const buf = Buffer.from(arr.buffer, arr.byteOffset, arr.byteLength);
            const downloadMs = Date.now() - t0;

            // Parse zip
            const zip = new AdmZip(buf);
            const entries = zip.getEntries();

            let xData: Float32Array | null = null;
            let yData: Float32Array | null = null;
            let xShape: number[] = [];
            let yShape: number[] = [];

            for (const entry of entries) {
                const name = entry.entryName;
                if (name === 'x.npy') {
                    const npyBuf = entry.getData();
                    const parsed = parseNpy(npyBuf);
                    xData = parsed.data;
                    xShape = parsed.shape;
                } else if (name === 'y.npy') {
                    const npyBuf = entry.getData();
                    const parsed = parseNpy(npyBuf);
                    yData = parsed.data;
                    yShape = parsed.shape;
                }
            }

            if (!xData || xShape.length !== 3) {
                structuredLog('error', 'npz_invalid_x', { key, xShape });
                return null;
            }

            const [bands, height, width] = xShape as [number, number, number];
            const result: ParsedNpz = {
                x: xData,
                y: yData || new Float32Array(height * width),
                bands,
                height,
                width,
            };

            npzCache.set(cacheKey, result);
            structuredLog('info', 'npz_loaded', {
                key, downloadMs, bands, height, width,
                xLen: xData.length, yLen: result.y.length,
            });
            return result;
        } catch (e) {
            structuredLog('error', 'npz_load_error', { key, error: (e as Error).message });
            return null;
        } finally {
            pendingLoads.delete(cacheKey);
        }
    })();

    pendingLoads.set(cacheKey, loadPromise);
    return loadPromise;
}

/**
 * De-normalization from GEE unitScale ranges.
 *
 * GEE pipeline (GEE.py / copernicus_tide.py):
 *   rainfall:     unitScale(0, 200)   → mm
 *   soilMoisture: unitScale(0, 0.5)   → m³/m³  (×100 for %)
 *   tide:         unitScale(-1.5, 1.5) → m
 *   flood:        binary probability   → 0-1 (no change)
 *   dem:          NOT real elevation in NPZ (spatially constant, varies by date)
 *   slope:        unitScale(0, 90)     → degrees (assumed)
 *   flow:         log10 + unitScale(0, 5) → log-scale accumulation
 *   landCover:    normalized class     → 0-1 index
 */
function denormalize(bandName: string, raw: number): number {
    switch (bandName) {
        case 'rainfall':     return raw * 200;                     // mm
        case 'soilMoisture': return raw * 0.5;                     // m³/m³ (volumetric)
        case 'tide':         return raw * 3.0 - 1.5;              // meters
        case 'slope':        return raw * 90;                      // degrees
        case 'flow':         return raw > 0 ? Math.pow(10, raw * 5) - 1 : 0; // accumulation cells
        case 'dem':          return raw;                           // keep as index (not real elevation)
        case 'landCover':    return raw;                           // keep as index
        case 'flood':        return raw;                           // probability 0-1
        default:             return raw;
    }
}

/**
 * Read pixel values from NPZ file on R2.
 * Returns de-normalized physical values keyed by STACKED_BAND_NAMES.
 */
export async function readPixelFromNpz(
    region: string,
    date: string,
    lat: number,
    lng: number,
): Promise<Record<string, number | null> | null> {
    const bounds = REGION_BOUNDS[region];
    if (!bounds) return null;

    const npz = await loadNpzFromR2(date);
    if (!npz) return null;

    const { x, bands, height, width } = npz;

    // Map lat/lng to row/col
    const col = Math.floor((lng - bounds.west) / (bounds.east - bounds.west) * width);
    const row = Math.floor((bounds.north - lat) / (bounds.north - bounds.south) * height);

    if (col < 0 || col >= width || row < 0 || row >= height) return null;

    const result: Record<string, number | null> = {};
    for (let i = 0; i < Math.min(bands, STACKED_BAND_NAMES.length); i++) {
        const bandName = STACKED_BAND_NAMES[i]!;
        const idx = i * height * width + row * width + col;
        const val = x[idx];
        if (val === undefined || isNaN(val)) {
            result[bandName] = null;
        } else {
            const physical = denormalize(bandName, val);
            result[bandName] = parseFloat(physical.toFixed(4));
        }
    }

    return result;
}
