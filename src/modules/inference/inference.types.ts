/**
 * Inference Types — Request/Response DTOs for ML prediction.
 */

export interface InferenceFeatures {
    rainfall: number;
    soilMoisture: number;
    tide: number;
    dem: number;
    slope: number;
    flow: number;
    landCover: number;
}

export interface InferencePrediction {
    flood_risk: 'LOW' | 'MEDIUM' | 'HIGH';
    probability: number[];
    confidence: number;
    model_version: string;
    features_used: string[];
}

export interface BatchPredictionResult {
    flood_risk: 'LOW' | 'MEDIUM' | 'HIGH';
    probability: number[];
    confidence: number;
}

export interface BatchPredictionResponse {
    predictions: BatchPredictionResult[];
    model_version: string;
    count: number;
}

export interface InferenceHealth {
    status: string;
    model_loaded: boolean;
    model_version: string;
    uptime_seconds: number;
}

export interface ModelInfo {
    model_version: string;
    model_type: string;
    feature_names: string[];
    trained_at: string | null;
    accuracy: number | null;
    f1_score: number | null;
}
