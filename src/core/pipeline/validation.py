from __future__ import annotations

import json
from hashlib import sha256
from typing import Any


class ValidationError(ValueError):
    """Raised when pipeline payload validation fails."""


def ensure_json_serializable(payload: Any) -> None:
    """Ensure payload can be serialized to JSON."""
    try:
        json.dumps(payload, sort_keys=True, separators=(",", ":"))
    except TypeError as exc:
        raise ValidationError("payload must be JSON-serializable") from exc


def ensure_hashable(payload: Any) -> str:
    """Ensure payload can be hashed deterministically; return hash string."""
    packed = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return sha256(packed.encode("utf-8")).hexdigest()


def ensure_comparable(payload: Any) -> None:
    """Ensure payload remains comparable after JSON round-trip."""
    packed = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if json.loads(packed) != payload:
        raise ValidationError("payload must be JSON-comparable")


def validate_payload(payload: Any) -> str:
    """Validate payload for JSON serialization, hashing, and comparability."""
    ensure_json_serializable(payload)
    ensure_comparable(payload)
    return ensure_hashable(payload)
