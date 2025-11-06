# core/cache.py
from __future__ import annotations
from typing import Dict, Any, Optional
import hashlib

_cache: Dict[str, Any] = {}

def file_hash(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def get(key: str) -> Optional[Any]:
    return _cache.get(key)

def set_(key: str, value: Any) -> None:
    _cache[key] = value
