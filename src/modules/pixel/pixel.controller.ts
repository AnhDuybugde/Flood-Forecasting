/**
 * Pixel Controller — Route handler with Zod validation.
 * Never throws. Uses Result pattern from service layer.
 */
import { Router } from 'express';
import { PixelParamsSchema } from './pixel.types';
import * as pixelService from './pixel.service';
import * as pixelHistoryService from './pixel.history';
import * as pixelBatchService from './pixel.batch';
import { ok, fail } from '../../shared/types/envelope';
import { z } from 'zod';
import { LatSchema, LngSchema, RegionSchema, DateStrSchema } from '../../shared/types/common';
import { structuredLog } from '../../shared/middleware/tracing';

const router = Router();

// POST /api/pixel/batch — Fetch multiple dates in one request
router.post('/pixel/batch', async (req, res) => {
    const BatchSchema = z.object({
        lat: LatSchema,
        lng: LngSchema,
        region: RegionSchema,
        dates: z.array(DateStrSchema).min(1).max(60),
    });

    const parsed = BatchSchema.safeParse(req.body);
    if (!parsed.success) {
        return fail(res, `Invalid batch params: ${parsed.error.message}`, 400, 'VALIDATION');
    }

    const result = await pixelBatchService.getBatchPixels(parsed.data);

    if (!result.ok) {
        return fail(res, result.error.message, result.error.statusCode, result.error.code);
    }

    res.setHeader('Cache-Control', 'public, max-age=3600, stale-while-revalidate=600');
    return ok(res, result.value);
});

// GET /api/pixel/history?lat=&lng=&region=&startDate=&endDate=
router.get('/pixel/history', async (req, res) => {
    const QuerySchema = z.object({
        lat: LatSchema,
        lng: LngSchema,
        region: RegionSchema,
        startDate: DateStrSchema,
        endDate: DateStrSchema
    });

    const parsed = QuerySchema.safeParse(req.query);
    if (!parsed.success) {
        return fail(res, `Invalid history query params: ${parsed.error.message}`, 400, 'VALIDATION');
    }

    const { lat, lng, region, startDate, endDate } = parsed.data;
    const result = await pixelHistoryService.getPixelHistory(region, lat, lng, startDate, endDate);

    if (!result.ok) {
        return fail(res, result.error.message, result.error.statusCode, result.error.code);
    }

    res.setHeader('Cache-Control', 'public, max-age=3600, stale-while-revalidate=600');
    return ok(res, result.value);
});

// GET /api/pixel/monthly?lat=&lng=&region=&years=2020,2021,2022,2023,2024,2025
// Returns monthly aggregated rainfall for seasonality chart
router.get('/pixel/monthly', async (req, res) => {
    const QuerySchema = z.object({
        lat: LatSchema,
        lng: LngSchema,
        region: RegionSchema,
        years: z.string().regex(/^[\d,]+$/, 'Comma-separated years expected')
    });

    const parsed = QuerySchema.safeParse(req.query);
    if (!parsed.success) {
        return fail(res, `Invalid monthly query params: ${parsed.error.message}`, 400, 'VALIDATION');
    }

    const { lat, lng, region, years: yearsStr } = parsed.data;
    const years = yearsStr.split(',').map(Number).filter(y => y >= 2000 && y <= 2100);

    if (years.length === 0) {
        return fail(res, 'No valid years provided', 400, 'VALIDATION');
    }

    const result = await pixelHistoryService.getMonthlyRainfall(region, lat, lng, years);

    if (!result.ok) {
        return fail(res, result.error.message, result.error.statusCode, result.error.code);
    }

    // Cache for 6 hours — historical monthly data changes rarely
    res.setHeader('Cache-Control', 'public, max-age=21600, stale-while-revalidate=3600');
    return ok(res, result.value);
});

// Normalize date to YYYY-MM-DD (accept DD/MM/YYYY from query or params)
function normalizeDateStr(value: string | undefined): string | null {
    const trimmed = String(value ?? '').trim();
    const yyyyMmDd = /^\d{4}-\d{2}-\d{2}$/;
    if (yyyyMmDd.test(trimmed)) return trimmed;
    const ddmmyyyy = trimmed.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (ddmmyyyy) {
        const [, d, m, y] = ddmmyyyy;
        const day = parseInt(d!, 10), month = parseInt(m!, 10), year = parseInt(y!, 10);
        if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
            return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
        }
    }
    return null;
}

router.get('/pixel/:lat/:lng/:date/:region', async (req, res) => {
    const raw = req.params as Record<string, string>;
    const normalizedDate = normalizeDateStr(raw.date);
    if (normalizedDate) (req.params as Record<string, string>).date = normalizedDate;

    const parsed = PixelParamsSchema.safeParse(req.params);
    if (!parsed.success) {
        const issue = parsed.error.issues[0];
        return fail(res, `${issue?.path.join('.')}: ${issue?.message}`, 400, 'VALIDATION');
    }

    const result = await pixelService.getPixel(parsed.data);

    if (!result.ok) {
        structuredLog('warn', 'pixel_err', {
            code: result.error.code,
            message: result.error.message,
            region: parsed.data.region,
            date: parsed.data.date,
        });
        return fail(res, result.error.message, result.error.statusCode, result.error.code);
    }

    res.setHeader('Cache-Control', 'public, max-age=3600, stale-while-revalidate=600');
    res.setHeader('X-Response-Time', `${result.value.metadata.responseTimeMs}ms`);
    res.setHeader('X-Trace-Id', result.value.metadata.traceId);
    return ok(res, result.value);
});

export { router as pixelRouter };
