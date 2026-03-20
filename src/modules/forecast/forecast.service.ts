/**
 * Forecast Service — Calculate macroscopic metrics for regions.
 */
import type { Result } from '../../shared/types/result';
import type { AppError } from '../../shared/types/result';
import { Ok, Err, AppErrors } from '../../shared/types/result';
import * as metadataService from '../metadata/metadata.service';
import * as gridService from '../grid/grid.service';
import { structuredLog } from '../../shared/middleware/tracing';
import { getLocalRainfallTotal, getLocalTideTotal } from '../../shared/legacy/npz-reader';
import { MemoryCache } from '../../shared/cache/memory-cache';

import { Region } from '../../shared/types/common';

const seasonalityCache = new MemoryCache<any>(10, 60 * 60 * 1000); // 1h

export async function getRainfallTrend(region: string, targetDate: string): Promise<Result<{ date: string; total: number }[], AppError>> {
    const t0 = Date.now();

    // 1. Get all available dates for the region
    const timelineResult = await metadataService.getDates(region);
    if (!timelineResult.ok) {
        return Err(timelineResult.error);
    }

    // Flatten nested availableDates to a sorted list
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

    // 2. Filter dates up to targetDate and get the last 7
    const pastDates = availableDates.filter(d => d <= targetDate);
    const selectedDates = pastDates.slice(-7);

    if (selectedDates.length === 0) {
        return Ok([]);
    }

    // 3. Fetch rainfall totals: try Grid JSON first, fall back to local NPZ
    const fetchPromises = selectedDates.map(async (dateStr) => {
        // Strategy A: Grid JSON from R2
        const gridResult = await gridService.getGrid({ region: region as 'DaNang', date: dateStr, layer: 'rain' });

        if (gridResult.ok && gridResult.value && gridResult.value.data) {
            const dataArr = gridResult.value.data;
            const nodata = gridResult.value.nodata ?? -9999;
            const scale = gridResult.value.scale ?? 1;

            let sum = 0;
            let count = 0;
            for (let i = 0; i < dataArr.length; i++) {
                const val = dataArr[i];
                if (val !== undefined && val !== null && val !== nodata && val >= 0) {
                    sum += val * scale;
                    count++;
                }
            }
            const avg = count > 0 ? sum / count : 0;
            return { date: dateStr, total: parseFloat(avg.toFixed(2)) };
        }

        // Strategy B: Local NPZ fallback
        const localTotal = await getLocalRainfallTotal(dateStr, region);
        if (localTotal !== null) {
            structuredLog('info', 'rainfall_trend_local_npz', { date: dateStr, total: localTotal });
            return { date: dateStr, total: localTotal };
        }

        return { date: dateStr, total: 0 };
    });

    const results = await Promise.all(fetchPromises);
    results.sort((a, b) => a.date.localeCompare(b.date));

    structuredLog('info', 'forecast_rainfall_trend', { region, targetDate, days: results.length, durationMs: Date.now() - t0 });

    return Ok(results);
}

export async function getSeasonalityTrend(region: string): Promise<Result<{ date: string; year: number; dayOfYear: number; rain: number; tide: number }[], AppError>> {
    const t0 = Date.now();
    const cacheKey = `seasonality_${region}`;
    const cached = seasonalityCache.get(cacheKey);
    if (cached) return Ok(cached);

    const timelineResult = await metadataService.getDates(region);
    if (!timelineResult.ok) return Err(timelineResult.error);

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

    const results: { date: string; year: number; dayOfYear: number; rain: number; tide: number }[] = [];
    const chunkSize = 20;

    for (let i = 0; i < availableDates.length; i += chunkSize) {
        const chunk = availableDates.slice(i, i + chunkSize);
        const fetchPromises = chunk.map(async (dateStr) => {
            const dateObj = new Date(dateStr);
            const startOfYear = new Date(dateObj.getFullYear(), 0, 0);
            const diff = dateObj.getTime() - startOfYear.getTime() + (startOfYear.getTimezoneOffset() - dateObj.getTimezoneOffset()) * 60 * 1000;
            const dayOfYear = Math.floor(diff / (1000 * 60 * 60 * 24));

            let localTotal = await getLocalRainfallTotal(dateStr, region);
            let localTideTotal = await getLocalTideTotal(dateStr, region);

            if (localTotal === null) {
                const gridResult = await gridService.getGrid({ region: region as 'DaNang', date: dateStr, layer: 'rain' });
                if (gridResult.ok && gridResult.value && gridResult.value.data) {
                    const dataArr = gridResult.value.data;
                    const scale = gridResult.value.scale ?? 1;
                    let sum = 0, count = 0;
                    for (let j = 0; j < dataArr.length; j++) {
                        const val = dataArr[j];
                        if (val !== undefined && val >= 0) { sum += val * scale; count++; }
                    }
                    localTotal = count > 0 ? parseFloat((sum / count).toFixed(2)) : 0;
                }
            }

            if (localTideTotal === null) {
                const gridResult = await gridService.getGrid({ region: region as 'DaNang', date: dateStr, layer: 'tide' });
                if (gridResult.ok && gridResult.value && gridResult.value.data) {
                    const dataArr = gridResult.value.data;
                    const scale = gridResult.value.scale ?? 1;
                    let sum = 0, count = 0;
                    for (let j = 0; j < dataArr.length; j++) {
                        const val = dataArr[j];
                        if (val !== undefined && val !== null) { sum += val * scale; count++; }
                    }
                    localTideTotal = count > 0 ? parseFloat((sum / count).toFixed(2)) : 0;
                }
            }

            return { date: dateStr, year: dateObj.getFullYear(), dayOfYear, rain: localTotal ?? 0, tide: localTideTotal ?? 0 };
        });
        results.push(...await Promise.all(fetchPromises));
    }

    results.sort((a, b) => a.date.localeCompare(b.date));
    seasonalityCache.set(cacheKey, results);
    structuredLog('info', 'forecast_seasonality_trend', { region, days: results.length, durationMs: Date.now() - t0 });
    return Ok(results);
}

