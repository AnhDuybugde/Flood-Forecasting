/**
 * Grid module types — Pre-built grid JSON schema.
 */
import { z } from 'zod';
import { RegionSchema, DateStrSchema, LayerSchema } from '../../shared/types/common';

export const GridParamsSchema = z.object({
    region: RegionSchema,
    date: DateStrSchema,
    layer: LayerSchema,
});

export type GridParams = z.infer<typeof GridParamsSchema>;

/**
 * Pre-built grid JSON format (created by pipeline, stored in R2).
 * Compact: flat row-major array. Uses Float32Array for efficiency (bin path).
 */
export interface GridJSON {
    v: number;
    region: string;
    date: string;
    layer: string;
    bounds: { n: number; s: number; e: number; w: number };
    size: { r: number; c: number };
    scale: number;
    nodata: number;
    data: number[] | Float32Array;
}
