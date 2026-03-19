/**
 * Metadata module types — Registry schema for write-time indexing.
 */
import { z } from 'zod';
import { RegionSchema } from '../../shared/types/common';

// ── Metadata Registry (stored as JSON in R2) ───────────────
export interface MetadataRegistry {
    version: number;
    region: string;
    updatedAt: string;
    checksum: string;
    dateRange: { start: string; end: string };
    totalDays: number;
    dates: string[];
    layers: {
        daily: string[];
        static: string[];
    };
    stacked: {
        available: string[];
        bandOrder: string[];
    };
}

// ── Request validation ─────────────────────────────────────
export const MetadataParamsSchema = z.object({
    region: RegionSchema,
});

export type MetadataParams = z.infer<typeof MetadataParamsSchema>;

// ── Response DTO ───────────────────────────────────────────
export interface MetadataResponse {
    region: string;
    dateRange: { start: string; end: string };
    totalDays: number;
    availableDates: Record<string, Record<string, number[]>>;
    dataSources: { type: string };
}
