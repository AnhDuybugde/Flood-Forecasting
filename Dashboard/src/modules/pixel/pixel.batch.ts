/**
 * Batch Pixel Service — Fetch pixel data for multiple dates in a single request.
 *
 * POST /api/pixel/batch
 * Body: { lat, lng, region, dates: ["2024-01-01", "2024-01-02", ...] }
 *
 * Benefits:
 *   - Single HTTP round-trip instead of N sequential requests
 *   - Controlled concurrency to avoid R2 socket exhaustion
 *   - Shared cache hits across batch items
 */
import type { Result } from '../../shared/types/result';
import type { AppError } from '../../shared/types/result';
import { Ok, Err, AppErrors } from '../../shared/types/result';
import { REGION_BOUNDS } from '../../shared/types/common';
import * as pixelService from './pixel.service';
import { structuredLog } from '../../shared/middleware/tracing';

const MAX_BATCH_SIZE = 60; // Max dates per request
const BATCH_CONCURRENCY = 10; // Concurrent pixel reads within a batch

export interface BatchPixelRequest {
    lat: number;
    lng: number;
    region: string;
    dates: string[];
}

export interface BatchPixelItem {
    date: string;
    rainfall: number | null;
    soilMoisture: number | null;
    tide: number | null;
    flood: number | null;
    dem: number | null;
    slope: number | null;
    flow: number | null;
    landCover: number | null;
    floodRisk: string;
}

export interface BatchPixelResponse {
    lat: number;
    lng: number;
    region: string;
    items: BatchPixelItem[];
    metadata: {
        requested: number;
        returned: number;
        responseTimeMs: number;
    };
}

export async function getBatchPixels(
    req: BatchPixelRequest
): Promise<Result<BatchPixelResponse, AppError>> {
    const t0 = Date.now();
    const { lat, lng, region, dates } = req;

    // Validation
    const bounds = REGION_BOUNDS[region];
    if (!bounds) return Err(AppErrors.validation(`Unknown region: ${region}`));

    if (lat < bounds.south || lat > bounds.north || lng < bounds.west || lng > bounds.east) {
        return Err(AppErrors.outOfBounds(`Coordinates (${lat}, ${lng}) outside ${region}`));
    }

    if (!dates || dates.length === 0) {
        return Err(AppErrors.validation('dates array is required and must not be empty'));
    }

    if (dates.length > MAX_BATCH_SIZE) {
        return Err(AppErrors.validation(`Maximum ${MAX_BATCH_SIZE} dates per batch request`));
    }

    // Process dates with controlled concurrency
    const items: BatchPixelItem[] = [];

    for (let i = 0; i < dates.length; i += BATCH_CONCURRENCY) {
        const batch = dates.slice(i, i + BATCH_CONCURRENCY);
        const batchResults = await Promise.all(
            batch.map(async (date) => {
                const result = await pixelService.getPixel({
                    region: region as any,
                    date,
                    lat,
                    lng,
                });

                if (result.ok) {
                    return {
                        date,
                        rainfall: result.value.rainfall,
                        soilMoisture: result.value.soilMoisture,
                        tide: result.value.tide,
                        flood: result.value.flood,
                        floodRisk: result.value.floodRisk,
                        dem: result.value.dem,
                        slope: result.value.slope,
                        flow: result.value.flow,
                        landCover: result.value.landCover,
                    };
                }

                return {
                    date,
                    rainfall: null,
                    soilMoisture: null,
                    tide: null,
                    flood: null,
                    floodRisk: 'LOW' as string,
                    dem: null,
                    slope: null,
                    flow: null,
                    landCover: null,
                };
            })
        );
        items.push(...batchResults);
    }

    items.sort((a, b) => a.date.localeCompare(b.date));

    const elapsed = Date.now() - t0;
    structuredLog('info', 'pixel_batch', {
        region, lat, lng,
        requested: dates.length,
        returned: items.length,
        durationMs: elapsed,
    });

    return Ok({
        lat,
        lng,
        region,
        items,
        metadata: {
            requested: dates.length,
            returned: items.length,
            responseTimeMs: elapsed,
        },
    });
}
