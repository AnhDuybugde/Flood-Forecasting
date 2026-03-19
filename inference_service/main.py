"""
ML Inference Service — FastAPI application for flood prediction.

Architecture: Option B from ml-inference-design.md
  User Click → Edge Worker → R2 (COG pixel read) → Inference API → Combined response

Endpoints:
  POST /predict         — Single pixel prediction
  POST /predict/batch   — Batch pixel prediction
  GET  /health          — Health check
  GET  /model/info      — Model metadata
"""
import os
import time
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

try:
    from .schemas import (
        PixelFeatures,
        BatchPredictRequest,
        PredictResponse,
        BatchPredictResponse,
        HealthResponse,
        ModelInfoResponse,
        PredictionResult,
    )
    from .model import flood_model, FEATURE_NAMES
except ImportError:
    from schemas import (
        PixelFeatures,
        BatchPredictRequest,
        PredictResponse,
        BatchPredictResponse,
        HealthResponse,
        ModelInfoResponse,
        PredictionResult,
    )
    from model import flood_model, FEATURE_NAMES

# ── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("inference")

# ── App Lifecycle ───────────────────────────────────────────
START_TIME = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load model into RAM at startup."""
    logger.info("Loading flood prediction model...")
    flood_model.load()
    logger.info(f"Model ready: {flood_model.model_type} {flood_model.version}")
    yield
    logger.info("Inference service shutting down.")


# ── FastAPI App ─────────────────────────────────────────────
app = FastAPI(
    title="Flood Prediction Inference API",
    description="Real-time ML inference for per-pixel flood risk prediction (Da Nang region)",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Endpoints ───────────────────────────────────────────────
@app.post("/predict", response_model=PredictResponse)
async def predict(features: PixelFeatures):
    """
    Predict flood risk for a single pixel.

    Input features (from COG pixel read):
    - rainfall: mm
    - soilMoisture: %
    - tide: m
    - dem: m (elevation)
    - slope: degrees
    - flow: accumulation
    - landCover: class index
    """
    if not flood_model.loaded:
        raise HTTPException(status_code=503, detail="Model not loaded yet")

    feature_dict = features.model_dump()
    flood_risk, probability, confidence = flood_model.predict(feature_dict)

    return PredictResponse(
        flood_risk=flood_risk,
        probability=probability,
        confidence=confidence,
        model_version=flood_model.version,
        features_used=FEATURE_NAMES,
    )


@app.post("/predict/batch", response_model=BatchPredictResponse)
async def predict_batch(request: BatchPredictRequest):
    """
    Batch prediction for multiple pixels.
    Useful for grid-level prediction maps.
    """
    if not flood_model.loaded:
        raise HTTPException(status_code=503, detail="Model not loaded yet")

    if len(request.features) > 1000:
        raise HTTPException(status_code=400, detail="Maximum 1000 pixels per batch")

    feature_dicts = [f.model_dump() for f in request.features]
    results = flood_model.predict_batch(feature_dicts)

    predictions = [
        PredictionResult(
            flood_risk=risk,
            probability=prob,
            confidence=conf,
        )
        for risk, prob, conf in results
    ]

    return BatchPredictResponse(
        predictions=predictions,
        model_version=flood_model.version,
        count=len(predictions),
    )


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return HealthResponse(
        status="ok" if flood_model.loaded else "loading",
        model_loaded=flood_model.loaded,
        model_version=flood_model.version,
        uptime_seconds=round(time.time() - START_TIME, 2),
    )


@app.get("/model/info", response_model=ModelInfoResponse)
async def model_info():
    """Return model metadata."""
    return ModelInfoResponse(
        model_version=flood_model.version,
        model_type=flood_model.model_type,
        feature_names=FEATURE_NAMES,
        trained_at=flood_model.metadata.get("trained_at"),
        accuracy=flood_model.metadata.get("accuracy"),
        f1_score=flood_model.metadata.get("f1_score"),
    )
