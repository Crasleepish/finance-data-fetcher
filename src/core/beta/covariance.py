from __future__ import annotations

import json
from typing import Any

import numpy as np


def pack_covariance(matrix: np.ndarray) -> bytes:
    """Pack a covariance matrix into upper-triangle column-major float32 bytes."""
    matrix = np.asarray(matrix, dtype=np.float32)
    n = matrix.shape[0]
    data: list[float] = []
    for col in range(n):
        for row in range(col + 1):
            data.append(float(matrix[row, col]))
    return np.asarray(data, dtype=np.float32).tobytes()


def unpack_covariance(blob: bytes, meta_json: str) -> np.ndarray:
    """Unpack covariance bytes into a symmetric matrix using metadata JSON."""
    meta = json.loads(meta_json)
    dtype = np.dtype(meta["dtype"])
    n = int(meta["n"])
    expected = n * (n + 1) // 2
    arr = np.frombuffer(blob, dtype=dtype)
    if arr.size != expected:
        raise ValueError(f"unexpected covariance length: {arr.size} (expected {expected})")
    matrix = np.zeros((n, n), dtype=dtype)
    idx = 0
    for col in range(n):
        for row in range(col + 1):
            value = arr[idx]
            matrix[row, col] = value
            if row != col:
                matrix[col, row] = value
            idx += 1
    return matrix


def covariance_to_json(matrix: np.ndarray) -> str:
    """Serialize covariance matrix to JSON for storage."""
    return json.dumps(matrix.tolist(), separators=(",", ":"))


def safe_value(value: Any) -> Any:
    """Return None for NaN-like values."""
    try:
        return None if np.isnan(value) else value
    except TypeError:
        return value
