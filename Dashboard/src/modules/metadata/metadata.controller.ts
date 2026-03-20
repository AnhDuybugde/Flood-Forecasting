/**
 * Metadata Controller — Date index + timeline endpoints.
 */
import { Router } from 'express';
import { MetadataParamsSchema } from './metadata.types';
import * as metadataService from './metadata.service';
import { ok, fail } from '../../shared/types/envelope';
import { VALID_REGIONS } from '../../shared/types/common';

const router = Router();

// GET /api/dates/:region
router.get('/dates/:region', async (req, res) => {
    const parsed = MetadataParamsSchema.safeParse(req.params);
    if (!parsed.success) {
        return fail(res, `Invalid region. Valid: ${VALID_REGIONS.join(', ')}`, 400, 'VALIDATION');
    }

    // --- REAL WEATHER DYNAMIC DATES ---
    // Instead of failing when R2 is missing, generate the next 5 days of dates
    // since we proxy the 5-day OpenWeather forecast.
    const dates: string[] = [];
    const now = new Date();
    for (let i = 0; i < 6; i++) {
        const d = new Date(now);
        d.setDate(now.getDate() + i);
        dates.push(d.toISOString().split('T')[0] as string);
    }
    
    function datesToNested(dArr: string[]): Record<string, Record<string, number[]>> {
        const result: Record<string, Record<string, number[]>> = {};
        for (const dStr of dArr) {
            const [year, month, day] = dStr.split('-');
            if (!year || !month || !day) continue;
            if (!result[year]) result[year] = {};
            if (!result[year]![month]) result[year]![month] = [];
            result[year]![month]!.push(parseInt(day, 10));
        }
        return result;
    }

    const mockStats = {
        region: parsed.data.region,
        dateRange: { start: dates[0] as string, end: dates[dates.length - 1] as string },
        totalDays: dates.length,
        availableDates: datesToNested(dates),
        dataSources: { type: 'openweather_live' },
    };

    res.setHeader('Cache-Control', 'public, max-age=600, stale-while-revalidate=300');
    return ok(res, mockStats);
});

// GET /api/timeline
router.get('/timeline', async (_req, res) => {
    const dates: string[] = [];
    const now = new Date();
    for (let i = 0; i < 6; i++) {
        const d = new Date(now);
        d.setDate(now.getDate() + i);
        dates.push(d.toISOString().split('T')[0] as string);
    }

    const mockTimeline = {
        dates,
        dateRange: { start: dates[0] as string, end: dates[dates.length - 1] as string },
        totalDays: dates.length,
        regions: { DaNang: true },
    };

    res.setHeader('Cache-Control', 'public, max-age=600, stale-while-revalidate=300');
    return ok(res, mockTimeline);
});

export { router as metadataRouter };
