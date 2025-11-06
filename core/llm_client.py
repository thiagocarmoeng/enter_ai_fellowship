# core/llm_client.py
from __future__ import annotations
import json, os
from typing import Dict, List

USAGE_ACCUM = {"prompt_tokens": 0, "completion_tokens": 0}

try:
    from openai import OpenAI
    from openai import BadRequestError
except Exception:
    OpenAI = None
    BadRequestError = Exception

SYSTEM_PROMPT = (
    "Você é um extrator de campos. Leia o CONTEXTO e preencha APENAS as chaves pedidas.\n"
    "- Responda em JSON válido.\n"
    "- Use string vazia \"\" se não tiver certeza.\n"
    "- Não invente valores.\n"
)

# ======= helpers de módulo (fora da classe!) =================================
def _accumulate_usage(usage) -> None:
    if not usage:
        return
    try:
        USAGE_ACCUM["prompt_tokens"]   += int(getattr(usage, "prompt_tokens", 0) or 0)
        USAGE_ACCUM["completion_tokens"] += int(getattr(usage, "completion_tokens", 0) or 0)
    except Exception:
        pass

def get_cost_summary() -> Dict[str, float]:
    in_p  = float(os.getenv("PRICE_IN_PER_1K",  "0"))
    out_p = float(os.getenv("PRICE_OUT_PER_1K", "0"))
    pt = USAGE_ACCUM["prompt_tokens"]
    ct = USAGE_ACCUM["completion_tokens"]
    usd = (pt/1000.0)*in_p + (ct/1000.0)*out_p
    return {"prompt_tokens": pt, "completion_tokens": ct, "usd_total": usd}

def reset_usage() -> None:
    USAGE_ACCUM["prompt_tokens"] = 0
    USAGE_ACCUM["completion_tokens"] = 0

# =============================================================================
class LLMClient:
    def __init__(self, model: str | None = None, provider: str | None = None):
        self.model = model or os.getenv("LLM_MODEL", "gpt-5-mini")
        self.provider = provider or os.getenv("LLM_PROVIDER", "openai")
        self.api_key = os.getenv("LLM_API_KEY", "")
        self.timeout = float(os.getenv("LLM_TIMEOUT", "1000"))

        if self.provider == "openai":
            if OpenAI is None:
                raise RuntimeError("Pacote 'openai' não instalado.")
            if not self.api_key:
                raise RuntimeError("LLM_API_KEY ausente. Configure via .env")
            os.environ["OPENAI_API_KEY"] = self.api_key
            self._client = OpenAI()
        else:
            raise NotImplementedError(f"Provider não suportado: {self.provider}")

    def _build_messages(self, label: str, schema_keys: List[str], missing_keys: List[str], context: str) -> list[dict]:
        user = (
            f"LABEL: {label}\n"
            f"TODAS_AS_CHAVES: {schema_keys}\n"
            f"CHAVES_PENDENTES: {missing_keys}\n\n"
            f"CONTEXTO:\n\"\"\"\n{context}\n\"\"\""
        )
        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ]

    def _build_prompt_text(self, label: str, schema_keys: List[str], missing_keys: List[str], context: str) -> str:
        # Versão texto único para Responses API
        return (
            f"{SYSTEM_PROMPT}\n\n"
            f"LABEL: {label}\n"
            f"TODAS_AS_CHAVES: {schema_keys}\n"
            f"CHAVES_PENDENTES: {missing_keys}\n\n"
            f"CONTEXTO:\n\"\"\"\n{context}\n\"\"\"\n"
            f"Responda APENAS um JSON com as chaves: {missing_keys}."
        )

    def _call_chat_completions(self, messages: list[dict]) -> str:
        resp = self._client.chat.completions.create(
            model=self.model,
            messages=messages,
        )
        _accumulate_usage(getattr(resp, "usage", None))
        return resp.choices[0].message.content or "{}"

    def _call_responses(self, prompt_text: str) -> str:
        resp = self._client.responses.create(
            model=self.model,
            input=prompt_text,
        )
        _accumulate_usage(getattr(resp, "usage", None))
        if hasattr(resp, "output_text") and resp.output_text:
            return resp.output_text
        try:
            return resp.output[0].content[0].text
        except Exception:
            pass
        if hasattr(resp, "choices") and resp.choices:
            try:
                return resp.choices[0].message.content
            except Exception:
                pass
        return "{}"

    def solve(self, label: str, schema_keys: List[str], missing_keys: List[str], context: str) -> Dict[str, str]:
        messages = self._build_messages(label, schema_keys, missing_keys, context)
        try:
            text = self._call_chat_completions(messages)
        except BadRequestError:
            prompt_text = self._build_prompt_text(label, schema_keys, missing_keys, context)
            text = self._call_responses(prompt_text)
        except Exception:
            prompt_text = self._build_prompt_text(label, schema_keys, missing_keys, context)
            text = self._call_responses(prompt_text)

        try:
            data = json.loads(text) 
            if not isinstance(data, dict):
                raise ValueError("not a dict")
        except Exception:
            return {k: "" for k in missing_keys}

        return {k: ("" if data.get(k) is None else str(data.get(k, ""))) for k in missing_keys}

    def get_cost_summary():
        # preços via .env (USD por 1k tokens)
        in_p  = float(os.getenv("PRICE_IN_PER_1K",  "0"))
        out_p = float(os.getenv("PRICE_OUT_PER_1K", "0"))
        pt = USAGE_ACCUM["prompt_tokens"]
        ct = USAGE_ACCUM["completion_tokens"]
        usd = (pt/1000.0)*in_p + (ct/1000.0)*out_p
        return {"prompt_tokens": pt, "completion_tokens": ct, "usd_total": usd}

    def reset_usage():
        USAGE_ACCUM["prompt_tokens"] = 0
        USAGE_ACCUM["completion_tokens"] = 0
