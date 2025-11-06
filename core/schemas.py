# core/schemas.py
from __future__ import annotations
from pydantic import BaseModel, Field, ConfigDict
from typing import Dict, Any, Optional

class ExtractRequest(BaseModel):
    label: str
    extraction_schema: Dict[str, str]
    pdf_path: str
    model_config = ConfigDict(extra="ignore")

class ExtractResponse(BaseModel):
    data: Dict[str, Optional[str]]
    meta: Dict[str, Any] = Field(default_factory=dict)
