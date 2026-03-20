/**
 * Metadata Service — Registry-based date index lookup.
 *
 * Strategy:
 *   1. Try metadata.json from R2 (write-time indexed by pipeline)
 *   2. Fallback to legacy R2 scan (ListObjectsV2)
 *   3. Cache result in L1 (TTL 5min)
 */
import type { Result } from '../../shared/types/result';
import type { AppError } from '../../shared/types/result';
import { Ok, Err, AppErrors } from '../../shared/types/result';
import { MemoryCache } from '../../shared/cache/memory-cache';
import { structuredLog } from '../../shared/middleware/tracing';
import type { MetadataRegistry, MetadataResponse } from './metadata.types';

// L1 cache: metadata registry (TTL 5 min)
const metadataCache = new MemoryCache<MetadataRegistry>(10, 5 * 60 * 1000);

// ── Dependencies (injected from legacy api.js) ─────────────
let _loadDateIndex: ((region: string) => Promise<any>) | null = null;
let _r2GetJson: ((key: string) => Promise<any>) | null = null;

export function injectDeps(deps: {
    loadDateIndex: (region: string) => Promise<any>;
    r2GetJson?: (key: string) => Promise<any>;
}) {
    _loadDateIndex = deps.loadDateIndex;
    _r2GetJson = deps.r2GetJson ?? null;
}

// ── Registry-based lookup ──────────────────────────────────
async function loadFromRegistry(region: string): Promise<MetadataRegistry | null> {
    const cacheKey = `registry_${region}`;
    const cached = metadataCache.get(cacheKey);
    if (cached) return cached;

    if (!_r2GetJson) return null;

    try {
        const key = `FloodData/${region}/metadata.json`;
        const registry = await _r2GetJson(key) as MetadataRegistry;
        if (registry && registry.version) {
            metadataCache.set(cacheKey, registry);
            structuredLog('info', 'metadata_registry_loaded', {
                region, version: registry.version, totalDays: registry.totalDays,
            });
            return registry;
        }
    } catch {
        // Registry not available yet — fallback
    }
    return null;
}

// ── Convert flat dates array to nested year/month/day ──────
function datesToNested(dates: string[]): Record<string, Record<string, number[]>> {
    const result: Record<string, Record<string, number[]>> = {};
    for (const d of dates) {
        const [year, month, day] = d.split('-');
        if (!year || !month || !day) continue;
        if (!result[year]) result[year] = {};
        if (!result[year]![month]) result[year]![month] = [];
        result[year]![month]!.push(parseInt(day, 10));
    }
    return result;
}

// ── Main Service Methods ───────────────────────────────────
export async function getDates(region: string): Promise<Result<MetadataResponse, AppError>> {
    const t0 = Date.now();

    // Strategy 1: Registry (O(1), pre-indexed by pipeline)
    const registry = await loadFromRegistry(region);
    if (registry) {
        const elapsed = Date.now() - t0;
        structuredLog('info', 'metadata_lookup', { region, source: 'registry', durationMs: elapsed });
        return Ok({
            region: registry.region,
            dateRange: registry.dateRange,
            totalDays: registry.totalDays,
            availableDates: datesToNested(registry.dates),
            dataSources: { type: 'registry' },
        });
    }

    // Strategy 2: Legacy R2 scan (fallback)
    if (_loadDateIndex) {
        try {
            const index = await _loadDateIndex(region);
            if (index) {
                const elapsed = Date.now() - t0;
                structuredLog('info', 'metadata_lookup', { region, source: 'r2_scan', durationMs: elapsed });
                return Ok({
                    region: index.region,
                    dateRange: index.date_range,
                    totalDays: index.total_days,
                    availableDates: index.available_dates,
                    dataSources: { type: index.data_source },
                });
            }
        } catch {
            // R2 scan failed — fall through to local NPZ
        }
    }

    // Strategy 3: Local NPZ file scan (offline fallback)
    const { listLocalNpzDates } = await import('../../shared/legacy/npz-reader');
    const localDates = listLocalNpzDates();
    if (localDates.length > 0) {
        const elapsed = Date.now() - t0;
        structuredLog('info', 'metadata_lookup', { region, source: 'local_npz', durationMs: elapsed, totalDays: localDates.length });
        return Ok({
            region,
            dateRange: { start: localDates[0]!, end: localDates[localDates.length - 1]! },
            totalDays: localDates.length,
            availableDates: datesToNested(localDates),
            dataSources: { type: 'local_npz' },
        });
    }

    return Err(AppErrors.notFound(`No data for region "${region}"`));
}

export async function getTimeline(): Promise<Result<{ dates: string[]; dateRange: { start: string; end: string }; totalDays: number; regions: Record<string, boolean> }, AppError>> {
    const dnResult = await getDates('DaNang');

    const allDates = new Set<string>();
    if (dnResult.ok) {
        const nested = dnResult.value.availableDates;
        for (const [year, months] of Object.entries(nested)) {
            for (const [month, days] of Object.entries(months)) {
                for (const day of days) {
                    allDates.add(`${year}-${month}-${String(day).padStart(2, '0')}`);
                }
            }
        }
    }

    const dates = Array.from(allDates).sort();
    return Ok({
        dates,
        dateRange: { start: dates[0] ?? '2020-01-01', end: dates[dates.length - 1] ?? '' },
        totalDays: dates.length,
        regions: { DaNang: dnResult.ok },
    });
}
