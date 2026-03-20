/**
 * Pixel History Service — Fetches data for a specific pixel across a date range.
 *
 * Uses pixelService.getPixel() for each date (reads stacked COG / legacy TIF)
 * instead of gridService (which requires pre-built grid JSON that may not exist).
 */
import type { Result } from '../../shared/types/result';
import type { AppError } from '../../shared/types/result';
import { Ok, Err, AppErrors } from '../../shared/types/result';
import * as metadataService from '../metadata/metadata.service';
import * as pixelService from './pixel.service';
import { structuredLog } from '../../shared/middleware/tracing';
import { REGION_BOUNDS, Region } from '../../shared/types/common';
import { MemoryCache } from '../../shared/cache/memory-cache';

export interface DailyPixelData {
    date: string;
    rainfall: number | null;
    soilMoisture: number | null;
    tide: number | null;
    flood: number | null;
    floodRisk: string;
    dem: number | null;
    slope: number | null;
    flow: number | null;
    landCover: number | null;
}

export async function getPixelHistory(region: string, lat: number, lng: number, startDateStr: string, endDateStr: string): Promise<Result<DailyPixelData[], AppError>> {
    const t0 = Date.now();
    const bounds = REGION_BOUNDS[region];
    if (!bounds) {
        return Err(AppErrors.validation(`Unknown region: ${region}`));
    }

    if (lat > bounds.north || lat < bounds.south || lng < bounds.west || lng > bounds.east) {
        return Err(AppErrors.validation(`Coordinates ${lat}, ${lng} are outside region boundary`));
    }

    // Get all available dates
    const timelineResult = await metadataService.getDates(region);
    if (!timelineResult.ok) {
        return Err(timelineResult.error);
    }

    // Flatten and filter dates
    const availableDates: string[] = [];
    const nested = timelineResult.value.availableDates;
    for (const year of Object.keys(nested).sort()) {
        const months = nested[year];
        if (!months) continue;
        for (const month of Object.keys(months).sort()) {
            const days = months[month]?.sort((a: number, b: number) => a - b) || [];
            for (const day of days) {
                availableDates.push(`${year}-${month.padStart(2, '0')}-${String(day).padStart(2, '0')}`);
            }
        }
    }

    const rangeDates = availableDates.filter(d => d >= startDateStr && d <= endDateStr);

    if (rangeDates.length === 0) {
        return Ok([]);
    }

    // Fetch full pixel data for each date using pixelService
    // This reads from stacked COG / legacy TIF which has actual data
    const CONCURRENCY = 15;
    const results: DailyPixelData[] = [];

    for (let i = 0; i < rangeDates.length; i += CONCURRENCY) {
        const batch = rangeDates.slice(i, i + CONCURRENCY);
        const batchResults = await Promise.all(
            batch.map(async (dateStr) => {
                const pixelResult = await pixelService.getPixel({
                    region: region as any,
                    date: dateStr,
                    lat,
                    lng,
                });

                if (pixelResult.ok) {
                    return {
                        date: dateStr,
                        rainfall: pixelResult.value.rainfall,
                        soilMoisture: pixelResult.value.soilMoisture,
                        tide: pixelResult.value.tide,
                        flood: pixelResult.value.flood,
                        floodRisk: pixelResult.value.floodRisk,
                        dem: pixelResult.value.dem,
                        slope: pixelResult.value.slope,
                        flow: pixelResult.value.flow,
                        landCover: pixelResult.value.landCover,
                    };
                }

                // If pixel read fails for this date, return nulls
                return {
                    date: dateStr,
                    rainfall: null,
                    soilMoisture: null,
                    tide: null,
                    flood: null,
                    floodRisk: 'LOW',
                    dem: null,
                    slope: null,
                    flow: null,
                    landCover: null,
                };
            })
        );
        results.push(...batchResults);
    }

    results.sort((a, b) => a.date.localeCompare(b.date));

    structuredLog('info', 'pixel_history', { region, lat, lng, days: results.length, durationMs: Date.now() - t0 });

    return Ok(results);
}

/* ─────────────────────────────────────────────────
   Monthly Aggregation (for Seasonality Chart)
   GET /api/pixel/monthly?lat=&lng=&region=&years=2020,2021,...
   Returns: { [year]: [12 monthly totals] }

   Strategy: Sample up to MAX_SAMPLE_PER_MONTH dates per month,
   then extrapolate to estimate the full monthly total.
   Example: 6 years × 12 months × 4 samples ≈ 288 rainfall reads vs per-day full history.
   ───────────────────────────────────────────────── */

export interface MonthlyRainfall {
    [year: string]: (number | null)[];
}

const monthlyCache = new MemoryCache<MonthlyRainfall>(50, 6 * 3600 * 1000); // 6h TTL

function sampleDates(dates: string[], maxSamples: number): string[] {
    if (dates.length <= maxSamples) return dates;
    // Evenly spaced sampling — always include first and last
    const step = (dates.length - 1) / (maxSamples - 1);
    const sampled: string[] = [];
    for (let i = 0; i < maxSamples; i++) {
        const d = dates[Math.round(i * step)];
        if (d) sampled.push(d);
    }
    return sampled;
}

export async function getMonthlyRainfall(
    region: string,
    lat: number,
    lng: number,
    years: number[]
): Promise<Result<MonthlyRainfall, AppError>> {
    const t0 = Date.now();
    const bounds = REGION_BOUNDS[region];
    if (!bounds) return Err(AppErrors.validation(`Unknown region: ${region}`));

    if (lat > bounds.north || lat < bounds.south || lng < bounds.west || lng > bounds.east) {
        return Err(AppErrors.validation(`Coordinates ${lat}, ${lng} outside region boundary`));
    }

    // Check cache
    const latKey = lat.toFixed(3);
    const lngKey = lng.toFixed(3);
    const cacheKey = `monthly_${region}_${latKey}_${lngKey}_${years.join(',')}`;
    const cached = monthlyCache.get(cacheKey);
    if (cached) {
        structuredLog('info', 'pixel_monthly_cached', { region, lat, lng, durationMs: Date.now() - t0 });
        return Ok(cached);
    }

    // Get available dates once
    const timelineResult = await metadataService.getDates(region);
    if (!timelineResult.ok) return Err(timelineResult.error);

    const nested = timelineResult.value.availableDates;

    // Build result structure and collect tasks
    const result: MonthlyRainfall = {};
    const MAX_SAMPLE_PER_MONTH = 4; // Sample 4 dates per month max (rainfall-only reads are fast)

    interface MonthTask {
        year: number;
        month: string;
        allDates: string[];      // All available dates for this month
        sampleDates: string[];   // Sampled dates to actually read
    }

    const monthTasks: MonthTask[] = [];

    for (const year of years) {
        result[year] = new Array(12).fill(null);
        const yearStr = String(year);
        const yearData = nested[yearStr];
        if (!yearData) continue;

        for (const monthStr of Object.keys(yearData).sort()) {
            const days = yearData[monthStr] || [];
            if (days.length === 0) continue;

            const allDates = days
                .sort((a: number, b: number) => a - b)
                .map((d: number) => `${yearStr}-${monthStr.padStart(2, '0')}-${String(d).padStart(2, '0')}`);

            const sampled = sampleDates(allDates, MAX_SAMPLE_PER_MONTH);

            monthTasks.push({ year, month: monthStr, allDates, sampleDates: sampled });
        }
    }

    // Process month tasks with limited concurrency (6 months × up to 4 date reads ≈ 24 parallel R2 reads max)
    const MONTH_CONCURRENCY = 6;

    for (let i = 0; i < monthTasks.length; i += MONTH_CONCURRENCY) {
        const batch = monthTasks.slice(i, i + MONTH_CONCURRENCY);

        await Promise.all(batch.map(async (task) => {
            let sampleTotal = 0;
            let sampleCount = 0;

            // Read only rainfall values for sampled dates (1 TIF per date instead of 8)
            const dayResults = await Promise.all(
                task.sampleDates.map(dateStr =>
                    pixelService.getRainfallOnly(region, dateStr, lat, lng)
                        .catch(() => null)
                )
            );

            for (const rainfall of dayResults) {
                if (rainfall !== null && rainfall !== undefined) {
                    sampleTotal += Math.max(0, rainfall);
                    sampleCount++;
                }
            }

            const monthIdx = parseInt(task.month, 10) - 1;
            if (sampleCount > 0) {
                // Extrapolate: (sample total / sample count) * total days in month
                const avgDailyRain = sampleTotal / sampleCount;
                const estimatedMonthTotal = avgDailyRain * task.allDates.length;
                const yearArr = result[task.year];
                if (yearArr) {
                    yearArr[monthIdx] = parseFloat(estimatedMonthTotal.toFixed(2));
                }
            }
        }));
    }

    monthlyCache.set(cacheKey, result);

    structuredLog('info', 'pixel_monthly', {
        region, lat, lng,
        years: years.join(','),
        totalMonths: monthTasks.length,
        totalSamples: monthTasks.reduce((s, t) => s + t.sampleDates.length, 0),
        durationMs: Date.now() - t0,
    });

    return Ok(result);
}
