/**
 * Flood Risk Grid Endpoint — GeoJSON grid with flood risk for an entire region.
 *
 * GET /api/v1/flood-risk?region=DaNang&date=2024-01-01
 *
 * Returns: GeoJSON FeatureCollection where each Feature is a grid cell with:
 *   - geometry: Point (cell center)
 *   - properties: { flood, rainfall, floodRisk, row, col }
 *
 * Performance:
 *   - Uses pre-built grid JSON from R2 when available
 *   - Falls back to stacked COG pixel reads
 *   - Response cached with CDN-friendly headers (30min)
 *   - Compressed via gzip (Express compression middleware)
 */
import { Router } from 'express';
import { z } from 'zod';
import { ok, fail } from '../../shared/types/envelope';
import {
    REGION_BOUNDS, RegionSchema, DateStrSchema, deriveFloodRisk,
} from '../../shared/types/common';
import * as gridService from '../grid/grid.service';
import { structuredLog } from '../../shared/middleware/tracing';

const router = Router();

const FloodRiskQuerySchema = z.object({
    region: RegionSchema,
    date: DateStrSchema,
});

interface FloodRiskFeature {
    type: 'Feature';
    geometry: { type: 'Point'; coordinates: [number, number] };
    properties: {
        row: number;
        col: number;
        flood: number | null;
        rainfall: number | null;
        floodRisk: string;
    };
}

interface FloodRiskGeoJSON {
    type: 'FeatureCollection';
    metadata: {
        region: string;
        date: string;
        bounds: { north: number; south: number; east: number; west: number };
        gridSize: { rows: number; cols: number };
        responseTimeMs: number;
    };
    features: FloodRiskFeature[];
}

router.get('/flood-risk', async (req, res) => {
    const t0 = Date.now();

    const parsed = FloodRiskQuerySchema.safeParse(req.query);
    if (!parsed.success) {
        return fail(res, `Invalid params: ${parsed.error.message}`, 400, 'VALIDATION');
    }

    const { region, date } = parsed.data;
    const bounds = REGION_BOUNDS[region];
    if (!bounds) {
        return fail(res, `Unknown region: ${region}`, 400, 'VALIDATION');
    }

    try {
        // Try to get flood (label) grid data
        const floodResult = await gridService.getGrid({
            region: region as any,
            date,
            layer: 'label' as any,
        });

        // Try to get rain grid data
        const rainResult = await gridService.getGrid({
            region: region as any,
            date,
            layer: 'rain' as any,
        });

        const floodData = floodResult.ok ? floodResult.value : null;
        const rainData = rainResult.ok ? rainResult.value : null;

        if (!floodData && !rainData) {
            return fail(res, `No flood/rain data for ${region} on ${date}`, 404, 'NOT_FOUND');
        }

        const rows = bounds.rows;
        const cols = bounds.cols;
        const latStep = (bounds.north - bounds.south) / rows;
        const lngStep = (bounds.east - bounds.west) / cols;

        const features: FloodRiskFeature[] = [];

        for (let r = 0; r < rows; r++) {
            for (let c = 0; c < cols; c++) {
                const idx = r * cols + c;

                // Cell center coordinates
                const cellLat = bounds.north - (r + 0.5) * latStep;
                const cellLng = bounds.west + (c + 0.5) * lngStep;

                // Extract values from grid data
                let flood: number | null = null;
                let rainfall: number | null = null;

                if (floodData?.data) {
                    const raw = floodData.data[idx];
                    const nodata = floodData.nodata ?? -9999;
                    const scale = floodData.scale ?? 1000;
                    if (raw !== null && raw !== undefined && raw !== nodata) {
                        flood = raw / scale;
                    }
                }

                if (rainData?.data) {
                    const raw = rainData.data[idx];
                    const nodata = rainData.nodata ?? -9999;
                    const scale = rainData.scale ?? 1000;
                    if (raw !== null && raw !== undefined && raw !== nodata) {
                        rainfall = raw / scale;
                    }
                }

                const floodRisk = deriveFloodRisk(flood, rainfall);

                // Only include cells with data or non-LOW risk
                if (flood !== null || rainfall !== null) {
                    features.push({
                        type: 'Feature',
                        geometry: {
                            type: 'Point',
                            coordinates: [cellLng, cellLat],
                        },
                        properties: {
                            row: r,
                            col: c,
                            flood,
                            rainfall,
                            floodRisk,
                        },
                    });
                }
            }
        }

        const elapsed = Date.now() - t0;
        const response: FloodRiskGeoJSON = {
            type: 'FeatureCollection',
            metadata: {
                region,
                date,
                bounds: { north: bounds.north, south: bounds.south, east: bounds.east, west: bounds.west },
                gridSize: { rows, cols },
                responseTimeMs: elapsed,
            },
            features,
        };

        structuredLog('info', 'flood_risk_grid', {
            region, date, features: features.length, durationMs: elapsed,
        });

        // CDN-friendly caching: 30 min + 10 min stale-while-revalidate
        res.setHeader('Cache-Control', 'public, max-age=1800, stale-while-revalidate=600');
        res.setHeader('Content-Type', 'application/geo+json');
        return ok(res, response);

    } catch (err) {
        structuredLog('error', 'flood_risk_error', { region, date, error: (err as Error).message });
        return fail(res, `Failed to generate flood risk grid: ${(err as Error).message}`, 500, 'INTERNAL');
    }
});

export { router as floodRiskRouter };
