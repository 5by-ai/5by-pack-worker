"""
Local Model Loader for 5BY Pack Worker
GATE: GATE-201-render-direct-model-runtime-v1

Strategy:
- Process당 1회 lazy load (per-job reload 금지)
- Global singleton pattern
- Model weights repo commit 금지 (runtime download)

Primary candidate: google/flan-t5-small
Status: 1차 직접 탑재 후보 (테스트 후 확정)
"""

import os
import time
import logging
from typing import Optional, Tuple, Any

logger = logging.getLogger('5by-pack-worker')

# Model configuration
MODEL_NAME = os.getenv('LOCAL_MODEL_NAME', 'google/flan-t5-small')

# Global singleton (process당 1회 로드)
_model = None
_tokenizer = None
_model_loaded = False
_load_latency_ms: Optional[int] = None


def get_model_status() -> dict:
    """Get current model loading status."""
    return {
        'model_name': MODEL_NAME,
        'loaded': _model_loaded,
        'load_latency_ms': _load_latency_ms
    }


def ensure_model_loaded() -> Tuple[Any, Any]:
    """
    Lazy load model (process당 1회만).

    Returns:
        Tuple of (model, tokenizer)

    Raises:
        RuntimeError: If model loading fails
    """
    global _model, _tokenizer, _model_loaded, _load_latency_ms

    if _model_loaded and _model is not None and _tokenizer is not None:
        return _model, _tokenizer

    logger.info(f'Loading local model: {MODEL_NAME}')
    start = time.time()

    try:
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

        _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        _model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME)

        _model_loaded = True
        _load_latency_ms = int((time.time() - start) * 1000)

        logger.info(f'Model loaded successfully in {_load_latency_ms}ms')
        return _model, _tokenizer

    except Exception as e:
        _model_loaded = False
        _load_latency_ms = int((time.time() - start) * 1000)
        logger.error(f'Model loading failed after {_load_latency_ms}ms: {e}')
        raise RuntimeError(f'model_load_failed: {e}') from e


def test_inference(text: str = "Summarize: Hello world") -> dict:
    """
    Test inference with minimal input.

    For load testing only - not connected to business logic.
    """
    model, tokenizer = ensure_model_loaded()

    start = time.time()
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=128)
    outputs = model.generate(**inputs, max_new_tokens=50)
    result = tokenizer.decode(outputs[0], skip_special_tokens=True)
    inference_ms = int((time.time() - start) * 1000)

    return {
        'input': text,
        'output': result,
        'inference_ms': inference_ms,
        'model_name': MODEL_NAME
    }
