/**
 * Inference Client — HTTP client to call the FastAPI inference service.
 *
 * Architecture (from design doc):
 *   User Click → Edge Worker → R2 (COG pixel read, ~50ms)
 *                             → Inference API (features → prediction, ~30ms)
 *                             → Combined response to user (~80ms total)
 *
 * Features:
 *   - Configurable inference URL (env: INFERENCE_API_URL)
 *   - Timeout handling (default 5s)
 *   - In-memory cache for predictions (TTL 24h)
 *   - Graceful fallback to rule-based if service unavailable
 */
import type { Result } from '../../shared/types/result';
import type { AppError } from '../../shared/types/result';
import { Ok, Err, AppErrors } from '../../shared/types/result';
import { MemoryCache } from '../../shared/cache/memory-cache';
import { structuredLog } from '../../shared/middleware/tracing';
import type {
    InferenceFeatures,
    InferencePrediction,
    BatchPredictionResponse,
    InferenceHealth,
    ModelInfo,
} from './inference.types';

// ── Config ─────────────────────────────────────────────────
const INFERENCE_URL = process.env.INFERENCE_API_URL || 'http://localhost:8001';
const INFERENCE_TIMEOUT = parseInt(process.env.INFERENCE_TIMEOUT_MS || '5000', 10);

// Cache: predict:{lat4}:{lng4}:{date} → TTL 24 hours
const predictionCache = new MemoryCache<InferencePrediction>(1000, 24 * 60 * 60 * 1000);

// Track service availability
let _serviceAvailable = true;
let _lastHealthCheck = 0;
const HEALTH_CHECK_INTERVAL = 60_000; // Re-check health every 60s after failure


// ── Internal fetch helper ──────────────────────────────────
async function inferencePost<T>(path: string, body: unknown): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), INFERENCE_TIMEOUT);

    try {
        const res = await fetch(`${INFERENCE_URL}${path}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            signal: controller.signal,
        });

        if (!res.ok) {
            const text = await res.text().catch(() => '');
            throw new Error(`Inference API ${res.status}: ${text}`);
        }

        return await res.json() as T;
    } finally {
        clearTimeout(timer);
    }
}

async function inferenceGet<T>(path: string): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), INFERENCE_TIMEOUT);

    try {
        const res = await fetch(`${INFERENCE_URL}${path}`, {
            method: 'GET',
            signal: controller.signal,
        });

        if (!res.ok) {
            throw new Error(`Inference API ${res.status}`);
        }

        return await res.json() as T;
    } finally {
        clearTimeout(timer);
    }
}


// ── Public API ─────────────────────────────────────────────

/**
 * Predict flood risk for a single pixel using ML model.
 *
 * Strategy:
 *   1. Check cache
 *   2. Call inference API
 *   3. Fallback to rule-based if API unavailable
 */
export async function predictFloodRisk(
    features: InferenceFeatures,
    cacheKeyHint?: { lat: number; lng: number; date: string },
): Promise<Result<InferencePrediction, AppError>> {
    const t0 = Date.now();

    // 1. Check cache
    if (cacheKeyHint) {
        const cacheKey = `predict:${cacheKeyHint.lat.toFixed(4)}:${cacheKeyHint.lng.toFixed(4)}:${cacheKeyHint.date}`;
        const cached = predictionCache.get(cacheKey);
        if (cached) {
            structuredLog('info', 'inference_cache_hit', { cacheKey });
            return Ok(cached);
        }
    }

    // 2. Check if service is available
    if (!_serviceAvailable) {
        const now = Date.now();
        if (now - _lastHealthCheck < HEALTH_CHECK_INTERVAL) {
            // Still in cooldown — use fallback
            return Ok(ruleFallback(features));
        }
        // Re-check health
        _lastHealthCheck = now;
    }

    // 3. Call inference API
    try {
        const prediction = await inferencePost<InferencePrediction>('/predict', features);

        // Cache result
        if (cacheKeyHint) {
            const cacheKey = `predict:${cacheKeyHint.lat.toFixed(4)}:${cacheKeyHint.lng.toFixed(4)}:${cacheKeyHint.date}`;
            predictionCache.set(cacheKey, prediction);
        }

        _serviceAvailable = true;

        structuredLog('info', 'inference_predict', {
            flood_risk: prediction.flood_risk,
            confidence: prediction.confidence,
            model_version: prediction.model_version,
            durationMs: Date.now() - t0,
        });

        return Ok(prediction);
    } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        structuredLog('warn', 'inference_fallback', { reason: msg });

        _serviceAvailable = false;
        _lastHealthCheck = Date.now();

        // Fallback to rule-based
        return Ok(ruleFallback(features));
    }
}


/**
 * Batch prediction for multiple pixels.
 */
export async function predictBatch(
    featuresList: InferenceFeatures[],
): Promise<Result<BatchPredictionResponse, AppError>> {
    if (!_serviceAvailable) {
        // Batch fallback
        const predictions = featuresList.map(f => {
            const { flood_risk, probability, confidence } = ruleFallback(f);
            return { flood_risk, probability, confidence };
        });
        return Ok({
            predictions: predictions as BatchPredictionResponse['predictions'],
            model_version: 'v0.0-fallback',
            count: predictions.length,
        });
    }

    try {
        const response = await inferencePost<BatchPredictionResponse>('/predict/batch', {
            features: featuresList,
        });
        return Ok(response);
    } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        structuredLog('warn', 'inference_batch_fallback', { reason: msg });

        const predictions = featuresList.map(f => {
            const { flood_risk, probability, confidence } = ruleFallback(f);
            return { flood_risk, probability, confidence };
        });
        return Ok({
            predictions: predictions as BatchPredictionResponse['predictions'],
            model_version: 'v0.0-fallback',
            count: predictions.length,
        });
    }
}


/**
 * Health check for the inference service.
 */
export async function checkHealth(): Promise<Result<InferenceHealth, AppError>> {
    try {
        const health = await inferenceGet<InferenceHealth>('/health');
        _serviceAvailable = health.model_loaded;
        return Ok(health);
    } catch {
        _serviceAvailable = false;
        return Ok({
            status: 'unavailable',
            model_loaded: false,
            model_version: 'N/A',
            uptime_seconds: 0,
        });
    }
}


/**
 * Get model information.
 */
export async function getModelInfo(): Promise<Result<ModelInfo, AppError>> {
    try {
        const info = await inferenceGet<ModelInfo>('/model/info');
        return Ok(info);
    } catch {
        return Err(AppErrors.internal('Inference service unavailable'));
    }
}


/**
 * Rule-based fallback (mirrors the Python rule_based_predict).
 */
function ruleFallback(features: InferenceFeatures): InferencePrediction {
    const { rainfall, soilMoisture, tide, dem, slope, flow } = features;

    let score = 0;

    // Rainfall
    if (rainfall > 100) score += 0.35;
    else if (rainfall > 50) score += 0.20;
    else if (rainfall > 20) score += 0.10;

    // Soil moisture
    if (soilMoisture > 80) score += 0.20;
    else if (soilMoisture > 50) score += 0.10;

    // Tide
    if (tide > 1.5) score += 0.15;
    else if (tide > 0.8) score += 0.08;

    // Elevation
    if (dem < 5) score += 0.15;
    else if (dem < 15) score += 0.08;

    // Slope
    if (slope < 2) score += 0.10;
    else if (slope < 5) score += 0.05;

    // Flow
    if (flow > 1000) score += 0.10;
    else if (flow > 100) score += 0.05;

    score = Math.min(score, 1.0);

    let flood_risk: 'LOW' | 'MEDIUM' | 'HIGH';
    if (score >= 0.6) flood_risk = 'HIGH';
    else if (score >= 0.3) flood_risk = 'MEDIUM';
    else flood_risk = 'LOW';

    return {
        flood_risk,
        probability: [1.0 - score, score],
        confidence: Math.max(1.0 - score, score),
        model_version: 'v0.0-fallback',
        features_used: ['rainfall', 'soilMoisture', 'tide', 'dem', 'slope', 'flow', 'landCover'],
    };
}


/** Expose cache size for monitoring */
export function getCacheSize(): number {
    return predictionCache.size;
}
