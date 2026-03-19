"""
Model management — loading, prediction, and versioning.
"""
import os
import json
import logging
import numpy as np
import joblib
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime

logger = logging.getLogger("inference.model")

FEATURE_NAMES = ["rainfall", "soilMoisture", "tide", "dem", "slope", "flow", "landCover"]
MODEL_DIR = os.path.join(os.path.dirname(__file__), "model")
DEFAULT_MODEL_PATH = os.path.join(MODEL_DIR, "flood_model.pkl")
META_PATH = os.path.join(MODEL_DIR, "model_meta.json")


class FloodModel:
    """Wrapper around the flood prediction model."""

    def __init__(self):
        self.model = None
        self.version: str = "v0.0"
        self.model_type: str = "unknown"
        self.feature_names: List[str] = FEATURE_NAMES
        self.metadata: Dict[str, Any] = {}
        self.loaded: bool = False
        self.model_path: str = ""
        self._torch = None
        self._is_torch: bool = False

    def load(self, model_path: Optional[str] = None, meta_path: str = META_PATH):
        """Load model from disk."""
        model_path = model_path or os.environ.get("MODEL_PATH") or os.environ.get("FLOOD_MODEL_PATH") or DEFAULT_MODEL_PATH
        self.model_path = model_path

        if not os.path.exists(model_path):
            logger.warning(f"Model file not found: {model_path}. Using rule-based fallback.")
            self.model = None
            self.version = "v0.0-fallback"
            self.model_type = "rule_based"
            self._is_torch = False
            self.loaded = True
            return

        try:
            ext = os.path.splitext(model_path)[1].lower()
            if ext in (".pth", ".pt"):
                self.model = self._load_torch_model(model_path)
                self._is_torch = True
                logger.info(f"PyTorch model loaded from {model_path}")
            else:
                self.model = joblib.load(model_path)
                self._is_torch = False
                logger.info(f"Joblib model loaded from {model_path}")

            # Load metadata if available
            if os.path.exists(meta_path):
                with open(meta_path, "r") as f:
                    self.metadata = json.load(f)
                self.version = self.metadata.get("version", "v1.0")
                self.model_type = self.metadata.get("model_type", "unknown")
            else:
                self.version = "v1.0"
                self.model_type = type(self.model).__name__ if self.model is not None else "unknown"

            self.loaded = True
            logger.info(f"Model ready: {self.model_type} {self.version}")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            self.model = None
            self.version = "v0.0-fallback"
            self.model_type = "rule_based"
            self._is_torch = False
            self.loaded = True

    def predict(self, features: Dict[str, float]) -> Tuple[str, List[float], float]:
        """
        Predict flood risk for a single pixel.

        Returns: (flood_risk, probability, confidence)
        """
        X = np.array([[features.get(f, 0.0) for f in FEATURE_NAMES]])

        if self.model is not None:
            if self._is_torch:
                return self._torch_predict(X)
            return self._ml_predict(X)
        else:
            return self._rule_based_predict(features)

    def predict_batch(self, feature_list: List[Dict[str, float]]) -> List[Tuple[str, List[float], float]]:
        """Batch prediction for multiple pixels."""
        if self.model is not None:
            X = np.array([[f.get(name, 0.0) for name in FEATURE_NAMES] for f in feature_list])
            if self._is_torch:
                return self._torch_predict_batch(X)
            return self._ml_predict_batch(X)
        else:
            return [self._rule_based_predict(f) for f in feature_list]

    def _load_torch_model(self, model_path: str):
        """
        Load a PyTorch model from .pth/.pt.

        Supported (best-effort):
        - torch.jit scripted module
        - torch.save(model) pickled nn.Module
        - torch.save({"model": model, ...})

        NOTE: If the file is a pure state_dict, the architecture is required and we
        cannot reconstruct it here without additional metadata/code.
        """
        try:
            import torch  # type: ignore
        except Exception as e:
            raise RuntimeError("PyTorch not installed. Add 'torch' to requirements.txt to load .pth models.") from e

        self._torch = torch

        # Prefer TorchScript if possible
        try:
            m = torch.jit.load(model_path, map_location="cpu")
            m.eval()
            return m
        except Exception:
            pass

        obj = torch.load(model_path, map_location="cpu")
        if hasattr(obj, "eval"):
            obj.eval()
            return obj
        if isinstance(obj, dict):
            for k in ("model", "net", "module"):
                m = obj.get(k)
                if m is not None and hasattr(m, "eval"):
                    m.eval()
                    return m
            raise RuntimeError(
                "Loaded a dict from .pth but couldn't find a model object. "
                "If this is a state_dict, you need to provide the model architecture code."
            )
        raise RuntimeError(f"Unsupported torch object type in {model_path}: {type(obj)}")

    def _torch_predict(self, X: np.ndarray) -> Tuple[str, List[float], float]:
        torch = self._torch
        if torch is None:
            raise RuntimeError("Torch runtime not initialized")

        with torch.no_grad():
            x = torch.tensor(X, dtype=torch.float32)
            out = self.model(x)

            # Normalize output shapes to probabilities [P(no_flood), P(flood)]
            prob = self._torch_output_to_proba(out)[0]
            confidence = float(max(prob))
            flood_risk = self._classify_risk(1 if prob[1] >= 0.5 else 0, prob)
            return flood_risk, [float(prob[0]), float(prob[1])], confidence

    def _torch_predict_batch(self, X: np.ndarray) -> List[Tuple[str, List[float], float]]:
        torch = self._torch
        if torch is None:
            raise RuntimeError("Torch runtime not initialized")

        with torch.no_grad():
            x = torch.tensor(X, dtype=torch.float32)
            out = self.model(x)
            probas = self._torch_output_to_proba(out)

        results: List[Tuple[str, List[float], float]] = []
        for i in range(probas.shape[0]):
            p0 = float(probas[i, 0].item())
            p1 = float(probas[i, 1].item())
            proba = [p0, p1]
            confidence = max(proba)
            flood_risk = self._classify_risk(1 if p1 >= 0.5 else 0, proba)
            results.append((flood_risk, proba, float(confidence)))
        return results

    def _torch_output_to_proba(self, out):
        """
        Convert common torch model outputs to a (N,2) probability array.
        Handles:
        - logits (N,) or (N,1) -> sigmoid
        - logits (N,2) -> softmax
        - probabilities already in [0,1]
        """
        torch = self._torch
        if torch is None:
            raise RuntimeError("Torch runtime not initialized")

        if isinstance(out, (tuple, list)):
            out = out[0]

        t = out
        if not hasattr(t, "shape"):
            t = torch.tensor(t)

        if t.dim() == 0:
            t = t.view(1, 1)

        if t.dim() == 1:
            # (N,) logits/prob for flood class
            flood = torch.sigmoid(t) if (t.min() < 0 or t.max() > 1) else t
            flood = flood.view(-1, 1)
            no_flood = 1.0 - flood
            return torch.cat([no_flood, flood], dim=1)

        if t.dim() == 2:
            if t.shape[1] == 1:
                flood = torch.sigmoid(t) if (t.min() < 0 or t.max() > 1) else t
                no_flood = 1.0 - flood
                return torch.cat([no_flood, flood], dim=1)
            if t.shape[1] == 2:
                return torch.softmax(t, dim=1) if (t.min() < 0 or t.max() > 1) else t

        # Fallback: flatten last dim
        t = t.view(t.shape[0], -1)
        if t.shape[1] >= 2:
            t2 = t[:, :2]
            return torch.softmax(t2, dim=1) if (t2.min() < 0 or t2.max() > 1) else t2

        flood = torch.sigmoid(t[:, 0]) if (t.min() < 0 or t.max() > 1) else t[:, 0]
        flood = flood.view(-1, 1)
        no_flood = 1.0 - flood
        return torch.cat([no_flood, flood], dim=1)

    def _ml_predict(self, X: np.ndarray) -> Tuple[str, List[float], float]:
        """ML model prediction."""
        prediction = int(self.model.predict(X)[0])

        if hasattr(self.model, "predict_proba"):
            proba = self.model.predict_proba(X)[0].tolist()
        else:
            # For models without predict_proba (e.g., SVM)
            proba = [1.0 - prediction, float(prediction)]

        confidence = max(proba)
        flood_risk = self._classify_risk(prediction, proba)
        return flood_risk, proba, confidence

    def _ml_predict_batch(self, X: np.ndarray) -> List[Tuple[str, List[float], float]]:
        """Batch ML prediction."""
        predictions = self.model.predict(X)

        if hasattr(self.model, "predict_proba"):
            probas = self.model.predict_proba(X)
        else:
            probas = np.column_stack([1.0 - predictions, predictions])

        results = []
        for i in range(len(predictions)):
            pred = int(predictions[i])
            proba = probas[i].tolist()
            confidence = max(proba)
            flood_risk = self._classify_risk(pred, proba)
            results.append((flood_risk, proba, confidence))

        return results

    def _classify_risk(self, prediction: int, proba: List[float]) -> str:
        """Classify flood risk level from prediction & probability."""
        if prediction == 1:
            flood_prob = proba[1] if len(proba) > 1 else proba[0]
            if flood_prob >= 0.8:
                return "HIGH"
            elif flood_prob >= 0.5:
                return "MEDIUM"
            else:
                return "LOW"
        return "LOW"

    def _rule_based_predict(self, features: Dict[str, float]) -> Tuple[str, List[float], float]:
        """
        Rule-based fallback when no ML model is available.
        Uses domain knowledge thresholds for flood risk assessment.
        """
        rainfall = features.get("rainfall", 0)
        soil_moisture = features.get("soilMoisture", 0)
        tide = features.get("tide", 0)
        dem = features.get("dem", 0)
        slope = features.get("slope", 0)
        flow = features.get("flow", 0)

        # Normalized risk score (0-1)
        score = 0.0

        # Rainfall contribution (biggest factor)
        if rainfall > 100:
            score += 0.35
        elif rainfall > 50:
            score += 0.20
        elif rainfall > 20:
            score += 0.10

        # Soil moisture (saturated soil = more runoff)
        if soil_moisture > 80:
            score += 0.20
        elif soil_moisture > 50:
            score += 0.10

        # Tide (high tide + rain = coastal flooding)
        if tide > 1.5:
            score += 0.15
        elif tide > 0.8:
            score += 0.08

        # Low elevation = higher risk
        if dem < 5:
            score += 0.15
        elif dem < 15:
            score += 0.08

        # Flat terrain = water accumulates
        if slope < 2:
            score += 0.10
        elif slope < 5:
            score += 0.05

        # High flow accumulation
        if flow > 1000:
            score += 0.10
        elif flow > 100:
            score += 0.05

        score = min(score, 1.0)
        proba = [1.0 - score, score]
        confidence = max(proba)

        if score >= 0.6:
            return "HIGH", proba, confidence
        elif score >= 0.3:
            return "MEDIUM", proba, confidence
        else:
            return "LOW", proba, confidence


# Singleton
flood_model = FloodModel()
