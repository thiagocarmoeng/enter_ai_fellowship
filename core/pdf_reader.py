# core/pdf_reader.py
from __future__ import annotations
from typing import List, Dict, Any
import fitz  
from pypdf import PdfReader

def extract_lines(pdf_path: str) -> List[str]:
    # 1) tenta PyMuPDF
    try:
        doc = fitz.open(pdf_path)
        lines: List[str] = []
        for page in doc:
            text = page.get_text("text")
            for ln in text.splitlines():
                s = ln.strip()
                if s:
                    lines.append(s)
        return lines
    except Exception:
        pass

    # 2) fallback: pypdf
    reader = PdfReader(pdf_path)
    lines: List[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        for ln in text.splitlines():
            s = ln.strip()
            if s:
                lines.append(s)
    return lines

def extract_blocks(pdf_path: str) -> List[Dict[str, Any]]:
    """Blocos com bbox quando PyMuPDF estiver disponível; senão, bloco simples por página."""
    try:
        doc = fitz.open(pdf_path)
        blocks = []
        for page in doc:
            for b in page.get_text("blocks"):
                x0, y0, x1, y1, text, *_ = b
                if text and text.strip():
                    blocks.append({
                        "text": text.strip(),
                        "x0": float(x0), "y0": float(y0),
                        "x1": float(x1), "y1": float(y1),
                    })
        return blocks
    except Exception:
        reader = PdfReader(pdf_path)
        blocks = []
        for page in reader.pages:
            text = (page.extract_text() or "").strip()
            if text:
                blocks.append({"text": text, "x0": 0.0, "y0": 0.0, "x1": 0.0, "y1": 0.0})
        return blocks
