# core/validators.py
from __future__ import annotations
import re
from typing import Optional, Dict

def normalize_whitespace(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return " ".join(s.split())

def post_normalize_by_key(key: str, value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = normalize_whitespace(value)
    k = key.lower()

    if "seccional" in k or k in {"uf", "u.f", "u.f."}:
        return v.replace("UF", "").replace(":", "").strip().upper()

    if "situacao" in k or "situação" in k:
        return v.upper()

    if "telefone" in k:
        digits = re.sub(r"[^0-9]", "", v)
        if len(digits) == 11:
            return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
        if len(digits) == 10:
            return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
        return v

    return v

def normalize_all(data: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    return {k: post_normalize_by_key(k, v) for k, v in data.items()}
