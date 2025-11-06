# core/layout_indexer.py
from __future__ import annotations
from typing import Dict, List, Iterable

def build_token_index(lines: List[str]) -> Dict[str, List[int]]:
    idx: Dict[str, List[int]] = {}
    for i, ln in enumerate(lines):
        for tok in ln.lower().replace(":", " ").split():
            idx.setdefault(tok, []).append(i)
    return idx

def find_anchor_lines(lines: List[str], variants: Iterable[str]) -> List[int]:
    res: List[int] = []
    vars_low = [v.lower() for v in variants]
    for i, ln in enumerate(lines):
        low = ln.lower()
        if any(v in low for v in vars_low):
            res.append(i)
    return res

def neighborhood(lines: List[str], center_idx: int, win: int = 2) -> List[str]:
    s = max(0, center_idx - win)
    e = min(len(lines), center_idx + win + 1)
    return lines[s:e]
