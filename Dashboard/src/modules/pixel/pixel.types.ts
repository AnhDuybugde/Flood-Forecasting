/**
 * Pixel module types — Zod schemas, DTOs, result types.
 */
import { z } from 'zod';
import { RegionSchema, DateStrSchema, LatSchema, LngSchema } from '../../shared/types/common';
import type { FloodRisk } from '../../shared/types/common';

// ── Request validation ─────────────────────────────────────
export const PixelParamsSchema = z.object({
    lat: LatSchema,
    lng: LngSchema,
    date: DateStrSchema,
    region: RegionSchema,
});

export type PixelParams = z.infer<typeof PixelParamsSchema>;

// ── Response DTO ───────────────────────────────────────────
export interface MlPrediction {
    flood_risk: 'LOW' | 'MEDIUM' | 'HIGH';
    probability: number[];
    confidence: number;
    model_version: string;
}

export interface PixelData {
    lat: number;
    lng: number;
    date: string;
    region: string;
    rainfall: number | null;
    soilMoisture: number | null;
    tide: number | null;
    flood: number | null;
    dem: number | null;
    slope: number | null;
    flow: number | null;
    landCover: number | null;
    floodRisk: FloodRisk;
    mlPrediction?: MlPrediction | null;
    bounds: { north: number; south: number; east: number; west: number };
    metadata: {
        source: 'npz' | 'stacked_cog' | 'legacy_8tif';
        traceId: string;
        responseTimeMs: number;
        cacheSize: { tif: number; pixel: number };
    };
}
