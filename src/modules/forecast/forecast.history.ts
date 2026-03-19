import { Result, Ok, Err, AppError, AppErrors } from '../../shared/types/result';
import * as metadataService from '../metadata/metadata.service';
import * as gridService from '../grid/grid.service';
import { REGION_BOUNDS, Region } from '../../shared/types/common';
import { structuredLog } from '../../shared/middleware/tracing';

export interface RegionHistoryData {
    date: string;
    totalRainfall: number | null;
    avgSoilMoisture: number | null;
    avgDem: number | null;
    avgSlope: number | null;
    avgFlow: number | null;
    avgLandCover: number | null;
}

/**
 * Calculates average/sum of a grid array ignoring nodata values.
 */
function calculateGridStats(
    dataArray: number[] | Float32Array | Float64Array | null | undefined,
    nodata: number,
    scale: number | undefined,
    type: 'sum' | 'avg' | 'mode'
): number | null {
    if (!dataArray || dataArray.length === 0) return null;

    let sum = 0;
    let count = 0;
    // For land cover mode approximation, we might just return the most frequent, 
    // but for simplicity, we'll return an average or just a generic stat.
    // SoilMoisture, DEM, Slope: Average
    // Rainfall, Flow: Sum

    for (let i = 0; i < dataArray.length; i++) {
        const val = dataArray[i];
        if (val !== undefined && val !== nodata && val > -9998) {
            sum += val;
            count++;
        }
    }

    if (count === 0) return null;

    let result = type === 'avg' ? (sum / count) : sum;
    if (scale) {
        result *= scale;
    }

    return parseFloat(result.toFixed(4));
}

export async function getRegionHistory(region: Region, startDateStr: string, endDateStr: string): Promise<Result<RegionHistoryData[], AppError>> {
    const t0 = Date.now();
    const bounds = REGION_BOUNDS[region];
    if (!bounds) return Err(AppErrors.validation(`Unknown region: ${region}`));

    // 1. Get timeline
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

    const rangeDates = availableDates.filter(d => d >= startDateStr && d <= endDateStr);
    if (rangeDates.length === 0) return Ok([]);

    // 2. Fetch static layers once (Since it's region level, calculating means of static maps)
    let avgDem: number | null = null;
    let avgSlope: number | null = null;
    let avgFlow: number | null = null;
    let avgLandCover: number | null = null;

    if (rangeDates.length > 0) {
        const firstDate = rangeDates[0];
        const [demRes, slopeRes, flowRes, lcRes] = await Promise.all([
            gridService.getGrid({ region: region as 'DaNang', date: firstDate as any, layer: 'dem' }),
            gridService.getGrid({ region: region as 'DaNang', date: firstDate as any, layer: 'slope' }),
            gridService.getGrid({ region: region as 'DaNang', date: firstDate as any, layer: 'flow' }),
            gridService.getGrid({ region: region as 'DaNang', date: firstDate as any, layer: 'landCover' })
        ]);

        if (demRes.ok && demRes.value && demRes.value.data) avgDem = calculateGridStats(demRes.value.data, demRes.value.nodata, demRes.value.scale, 'avg');
        if (slopeRes.ok && slopeRes.value && slopeRes.value.data) avgSlope = calculateGridStats(slopeRes.value.data, slopeRes.value.nodata, slopeRes.value.scale, 'avg');
        if (flowRes.ok && flowRes.value && flowRes.value.data) avgFlow = calculateGridStats(flowRes.value.data, flowRes.value.nodata, flowRes.value.scale, 'sum');
        if (lcRes.ok && lcRes.value && lcRes.value.data) avgLandCover = calculateGridStats(lcRes.value.data, lcRes.value.nodata, lcRes.value.scale, 'avg');
    }

    // 3. Fetch dynamic (daily) data
    const fetchPromises = rangeDates.map(async (dateStr) => {
        let totalRainfall: number | null = null;
        let avgSoilMoisture: number | null = null;

        const [rainRes, smRes] = await Promise.all([
            gridService.getGrid({ region: region as 'DaNang', date: dateStr as any, layer: 'rain' }),
            gridService.getGrid({ region: region as 'DaNang', date: dateStr as any, layer: 'soilMoisture' })
        ]);

        if (rainRes.ok && rainRes.value && rainRes.value.data) {
            totalRainfall = calculateGridStats(rainRes.value.data, rainRes.value.nodata, rainRes.value.scale, 'sum');
        }

        if (smRes.ok && smRes.value && smRes.value.data) {
            avgSoilMoisture = calculateGridStats(smRes.value.data, smRes.value.nodata, smRes.value.scale, 'avg');
        }

        return {
            date: dateStr,
            totalRainfall,
            avgSoilMoisture,
            avgDem,
            avgSlope,
            avgFlow,
            avgLandCover
        };
    });

    const results = await Promise.all(fetchPromises);
    results.sort((a, b) => a.date.localeCompare(b.date));

    structuredLog('info', 'region_history', { region, days: results.length, durationMs: Date.now() - t0 });
    return Ok(results);
}
