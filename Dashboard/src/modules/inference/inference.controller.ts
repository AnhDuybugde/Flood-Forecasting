/**
 * Inference Controller — API endpoints for ML flood prediction.
 *
 * Endpoints:
 *   POST /api/inference/predict          — Single pixel prediction
 *   POST /api/inference/predict/batch    — Batch prediction
 *   GET  /api/inference/health           — Inference service health
 *   GET  /api/inference/model            — Model info
 *   GET  /api/inference/pixel/:lat/:lng/:date/:region — Pixel data + ML prediction
 */
import { Router } from 'express';
import { z } from 'zod';
import { ok, fail } from '../../shared/types/envelope';
import { LatSchema, LngSchema, RegionSchema, DateStrSchema } from '../../shared/types/common';
import * as inferenceClient from './inference.client';
import * as pixelService from '../pixel/pixel.service';
import * as metadataService from '../metadata/metadata.service';
import type { InferenceFeatures } from './inference.types';

const router = Router();

function flattenAvailableDates(nested: Record<string, Record<string, number[]>>): string[] {
    const dates: string[] = [];
    for (const year of Object.keys(nested).sort()) {
        const months = nested[year];
        if (!months) continue;
        for (const month of Object.keys(months).sort()) {
            const days = months[month]?.sort((a: number, b: number) => a - b) || [];
            for (const day of days) {
                dates.push(`${year}-${month.padStart(2, '0')}-${String(day).padStart(2, '0')}`);
            }
        }
    }
    return dates;
}

// ── Schemas ────────────────────────────────────────────────
const FeaturesSchema = z.object({
    rainfall: z.coerce.number().default(0),
    soilMoisture: z.coerce.number().default(0),
    tide: z.coerce.number().default(0),
    dem: z.coerce.number().default(0),
    slope: z.coerce.number().default(0),
    flow: z.coerce.number().default(0),
    landCover: z.coerce.number().default(0),
});

const BatchSchema = z.object({
    features: z.array(FeaturesSchema).min(1).max(1000),
    region: z.string().optional(),
    date: z.string().optional(),
});

// ── GET /api/inference/pixel/latest5/:lat/:lng/:region ──────
// Returns pixel data + ML prediction for 5 most recent available dates
router.get('/pixel/latest5/:lat/:lng/:region', async (req, res) => {
    const ParamsSchema = z.object({
        lat: LatSchema,
        lng: LngSchema,
        region: RegionSchema,
    });

    const parsed = ParamsSchema.safeParse(req.params);
    if (!parsed.success) {
        return fail(res, `Invalid params: ${parsed.error.message}`, 400, 'VALIDATION');
    }

    const { lat, lng, region } = parsed.data;

    const timelineResult = await metadataService.getDates(region);
    if (!timelineResult.ok) {
        return fail(res, timelineResult.error.message, timelineResult.error.statusCode, timelineResult.error.code);
    }

    const allDates = flattenAvailableDates(timelineResult.value.availableDates);
    const selectedDates = allDates.slice(-5);
    if (selectedDates.length === 0) {
        return ok(res, { region, lat, lng, dates: [], count: 0 });
    }

    const items = await Promise.all(
        selectedDates.map(async (date) => {
            const pixelResult = await pixelService.getPixel({ lat, lng, date, region });
            if (!pixelResult.ok) {
                return { date, pixel: null, mlPrediction: null, error: pixelResult.error.message };
            }

            const pixelData = pixelResult.value;
            const features: InferenceFeatures = {
                rainfall: pixelData.rainfall ?? 0,
                soilMoisture: pixelData.soilMoisture ?? 0,
                tide: pixelData.tide ?? 0,
                dem: pixelData.dem ?? 0,
                slope: pixelData.slope ?? 0,
                flow: pixelData.flow ?? 0,
                landCover: pixelData.landCover ?? 0,
            };

            const predResult = await inferenceClient.predictFloodRisk(features, { lat, lng, date });
            return {
                date,
                pixel: pixelData,
                mlPrediction: predResult.ok ? predResult.value : null,
                error: predResult.ok ? null : predResult.error.message,
            };
        })
    );

    res.setHeader('Cache-Control', 'public, max-age=900, stale-while-revalidate=600');
    return ok(res, { region, lat, lng, dates: selectedDates, count: items.length, items });
});


// ── POST /api/inference/predict ────────────────────────────
router.post('/predict', async (req, res) => {
    const parsed = FeaturesSchema.safeParse(req.body);
    if (!parsed.success) {
        return fail(res, `Invalid features: ${parsed.error.message}`, 400, 'VALIDATION');
    }

    const result = await inferenceClient.predictFloodRisk(parsed.data as InferenceFeatures);
    if (!result.ok) {
        return fail(res, result.error.message, result.error.statusCode, result.error.code);
    }

    res.setHeader('Cache-Control', 'public, max-age=3600');
    return ok(res, result.value);
});


// ── POST /api/inference/predict/batch ──────────────────────
router.post('/predict/batch', async (req, res) => {
    const parsed = BatchSchema.safeParse(req.body);
    if (!parsed.success) {
        return fail(res, `Invalid batch request: ${parsed.error.message}`, 400, 'VALIDATION');
    }

    const result = await inferenceClient.predictBatch(parsed.data.features as InferenceFeatures[]);
    if (!result.ok) {
        return fail(res, result.error.message, result.error.statusCode, result.error.code);
    }

    return ok(res, result.value);
});


// ── GET /api/inference/pixel/:lat/:lng/:date/:region ───────
// Combined endpoint: reads pixel data from COG + returns ML prediction
router.get('/pixel/:lat/:lng/:date/:region', async (req, res) => {
    const ParamsSchema = z.object({
        lat: LatSchema,
        lng: LngSchema,
        date: DateStrSchema,
        region: RegionSchema,
    });

    const parsed = ParamsSchema.safeParse(req.params);
    if (!parsed.success) {
        return fail(res, `Invalid params: ${parsed.error.message}`, 400, 'VALIDATION');
    }

    const { lat, lng, date, region } = parsed.data;

    // 1. Read pixel data from COG/TIF (existing service)
    const pixelResult = await pixelService.getPixel({ lat, lng, date, region });
    if (!pixelResult.ok) {
        return fail(res, pixelResult.error.message, pixelResult.error.statusCode, pixelResult.error.code);
    }

    const pixelData = pixelResult.value;

    // 2. Build features from pixel data
    const features: InferenceFeatures = {
        rainfall: pixelData.rainfall ?? 0,
        soilMoisture: pixelData.soilMoisture ?? 0,
        tide: pixelData.tide ?? 0,
        dem: pixelData.dem ?? 0,
        slope: pixelData.slope ?? 0,
        flow: pixelData.flow ?? 0,
        landCover: pixelData.landCover ?? 0,
    };

    // 3. Get ML prediction
    const predResult = await inferenceClient.predictFloodRisk(features, { lat, lng, date });

    // 4. Combine pixel data + prediction
    const combined = {
        ...pixelData,
        mlPrediction: predResult.ok ? predResult.value : null,
    };

    res.setHeader('Cache-Control', 'public, max-age=3600, stale-while-revalidate=600');
    return ok(res, combined);
});


// ── GET /api/inference/health ──────────────────────────────
router.get('/health', async (_req, res) => {
    const result = await inferenceClient.checkHealth();
    if (!result.ok) {
        return fail(res, result.error.message, result.error.statusCode, result.error.code);
    }
    return ok(res, result.value);
});


// ── GET /api/inference/model ───────────────────────────────
router.get('/model', async (_req, res) => {
    const result = await inferenceClient.getModelInfo();
    if (!result.ok) {
        return fail(res, result.error.message, result.error.statusCode, result.error.code);
    }
    return ok(res, result.value);
});


export { router as inferenceRouter };
