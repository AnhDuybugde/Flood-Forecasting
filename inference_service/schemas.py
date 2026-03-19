"""
Pydantic schemas for the inference API.
"""
from pydantic import BaseModel, Field
from typing import Optional, List


class PixelFeatures(BaseModel):
    """Input features for a single pixel flood prediction."""
    rainfall: float = Field(0.0, description="Rainfall in mm")
    soilMoisture: float = Field(0.0, description="Soil moisture (%)")
    tide: float = Field(0.0, description="Tide level (m)")
    dem: float = Field(0.0, description="Digital Elevation Model (m)")
    slope: float = Field(0.0, description="Terrain slope (degrees)")
    flow: float = Field(0.0, description="Flow accumulation")
    landCover: float = Field(0.0, description="Land cover class")


class BatchPredictRequest(BaseModel):
    """Batch prediction request."""
    features: List[PixelFeatures]
    region: Optional[str] = "DaNang"
    date: Optional[str] = None


class PredictionResult(BaseModel):
    """Single prediction result."""
    flood_risk: str = Field(..., description="LOW / MEDIUM / HIGH")
    probability: List[float] = Field(..., description="[P(no_flood), P(flood)]")
    confidence: float = Field(..., description="Prediction confidence (0-1)")


class PredictResponse(BaseModel):
    """Response from /predict endpoint."""
    flood_risk: str
    probability: List[float]
    confidence: float
    model_version: str
    features_used: List[str]


class BatchPredictResponse(BaseModel):
    """Response from /predict/batch endpoint."""
    predictions: List[PredictionResult]
    model_version: str
    count: int


class HealthResponse(BaseModel):
    """Response from /health endpoint."""
    status: str
    model_loaded: bool
    model_version: str
    uptime_seconds: float


class ModelInfoResponse(BaseModel):
    """Response from /model/info endpoint."""
    model_version: str
    model_type: str
    feature_names: List[str]
    trained_at: Optional[str] = None
    accuracy: Optional[float] = None
    f1_score: Optional[float] = None
