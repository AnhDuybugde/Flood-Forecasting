/**
 * Common domain types — Branded types + Zod schemas for type-safe boundaries.
 */
import { z } from 'zod';

// ── Branded Types ──────────────────────────────────────────
type Brand<K, T extends string> = K & { readonly __brand: T };

export type Region = Brand<string, 'Region'>;
export type DateStr = Brand<string, 'DateStr'>;
export type R2Key = Brand<string, 'R2Key'>;

// ── Zod Schemas (validation at API boundary) ───────────────
export const RegionSchema = z.preprocess(
    (val) => {
        if (typeof val === 'string') {
            const lower = val.toLowerCase();
            if (lower === 'da-nang' || lower === 'danang') return 'DaNang';
        }
        return val;
    },
    z.enum(['DaNang'])
);
export const DateStrSchema = z.string().regex(
    /^\d{4}-\d{2}-\d{2}$/,
    'Date must be YYYY-MM-DD format'
);
export const LatSchema = z.coerce.number().min(-90).max(90);
export const LngSchema = z.coerce.number().min(-180).max(180);
export const LayerSchema = z.enum(['rain', 'soilMoisture', 'tide', 'label', 'dem', 'slope', 'flow', 'landCover']);

// ── Constants ──────────────────────────────────────────────
export const VALID_REGIONS = ['DaNang'] as const;

export const REGION_BOUNDS: Record<string, RegionBounds> = {
    DaNang: {
        north: 16.25, south: 15.95,
        east: 108.40, west: 107.90,
        rows: 20, cols: 20,
    },
};

export interface RegionBounds {
    north: number;
    south: number;
    east: number;
    west: number;
    rows: number;
    cols: number;
}

// ── Band Configuration (matches merge_multiband.py) ────────
export const STACKED_BAND_NAMES = [
    'rainfall', 'soilMoisture', 'tide', 'flood',
    'dem', 'slope', 'flow', 'landCover',
] as const;

export const STACKED_BAND_SCALES = [1000, 1000, 1000, 1000, 1, 1, 1, 1] as const;

export type BandName = typeof STACKED_BAND_NAMES[number];

export interface LayerConfig {
    sub: string;
    folder?: string;
    prefix: string;
    scale: number;
    isFlat?: boolean;
}

export const LAYER_FOLDER_MAP: Record<string, LayerConfig> = {
    rain: { sub: 'Daily', folder: 'Rain', prefix: 'Rain', scale: 1000 },
    soilMoisture: { sub: 'Daily', folder: 'SoilMoisture', prefix: 'SoilMoisture', scale: 1000 },
    tide: { sub: 'Daily', folder: 'Tide', prefix: 'Tide', scale: 1000 },
    label: { sub: 'LabelDaily', folder: '', prefix: 'Flood', scale: 1000 },
    dem: { sub: 'Static', prefix: 'DEM', isFlat: true, scale: 1 },
    slope: { sub: 'Static', prefix: 'Slope', isFlat: true, scale: 1 },
    flow: { sub: 'Static', prefix: 'Flow', isFlat: true, scale: 1 },
    landCover: { sub: 'Static', prefix: 'LandCover', isFlat: true, scale: 1 },
};

// ── Flood Risk Derivation ──────────────────────────────────
export type FloodRisk = 'LOW' | 'MEDIUM' | 'HIGH';

export function deriveFloodRisk(
    flood: number | null,
    rainfall: number | null,
): FloodRisk {
    if (flood !== null && flood > 0.5) return 'HIGH';
    if (rainfall !== null && rainfall > 80) return 'HIGH';
    if (rainfall !== null && rainfall > 40) return 'MEDIUM';
    return 'LOW';
}
