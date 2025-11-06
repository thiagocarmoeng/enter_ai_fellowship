# app/api.py
from __future__ import annotations

import os
import re
import shutil
import tempfile
import json
from typing import Optional, Dict, Tuple

from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse

from fastapi.staticfiles import StaticFiles

# --- Novo: carregue .env para a API também ---
try:
    from dotenv import load_dotenv
    load_dotenv()  # carrega variáveis de ambiente do .env
except Exception:
    pass

from core.orchestrator import run_extract
from core.pdf_reader import extract_lines
from core.field_matcher import detect_tela_layout as fm_detect_layout 

# ---------------- Config ----------------
EXTRACT_MIN_COVERAGE = float(os.getenv("EXTRACT_MIN_COVERAGE", "0.90"))
API_DEBUG = os.getenv("API_DEBUG", "0") == "1"
API_FORCE_LAYOUT = os.getenv("API_FORCE_LAYOUT", "").lower()  # "v1"|"v2"|"v3" para testes
DEFAULT_USE_LLM = (os.getenv("EXTRACT_USE_LLM", "0") == "1")

def _llm_env_ok() -> bool:
    return bool(
        os.getenv("LLM_API_KEY") or
        os.getenv("OPENAI_API_KEY") or
        os.getenv("AZURE_OPENAI_API_KEY")
    )

def _llm_meta():
    return {
        "provider": os.getenv("LLM_PROVIDER") or "",
        "model": os.getenv("LLM_MODEL") or "",
        "available": _llm_env_ok(),
    }

app = FastAPI(title="ENTER Extractor API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- Layout helpers ----------------
V1_KEYS_CANON = ("data_base", "data_vencimento", "quantidade_parcelas",
                 "produto", "sistema", "tipo_de_operacao", "tipo_de_sistema")
V1_KEYS_TYPO  = ("data_base", "data_verncimento", "quantidade_parcelas",
                 "produto", "sistema", "tipo_de_operacao", "tipo_de_sistema")
V2_KEYS = ("pesquisa_por", "pesquisa_tipo", "sistema", "valor_parcela", "cidade")
V3_KEYS = ("data_referencia", "selecao_de_parcelas", "total_de_parcelas")

def _score(values: dict, keys: Tuple[str, ...]) -> int:
    return sum(1 for k in keys if values.get(k))

def _score_v1(values: dict) -> int:
    return max(_score(values, V1_KEYS_CANON), _score(values, V1_KEYS_TYPO))

def _subset_for_layout(values: dict, layout: str) -> dict:
    if layout == "v1":
        keys_v1 = V1_KEYS_TYPO if _score(values, V1_KEYS_TYPO) >= _score(values, V1_KEYS_CANON) else V1_KEYS_CANON
        order = keys_v1
    elif layout == "v2":
        order = V2_KEYS
    else:
        order = V3_KEYS
    return {k: (values.get(k) or "") for k in order}

def _coverage(values: dict, layout: str) -> float:
    if layout == "v1":
        keys = V1_KEYS_TYPO if _score(values, V1_KEYS_TYPO) >= _score(values, V1_KEYS_CANON) else V1_KEYS_CANON
    elif layout == "v2":
        keys = V2_KEYS
    else:
        keys = V3_KEYS
    total = len(keys)
    filled = sum(1 for k in keys if values.get(k) not in (None, ""))
    return (filled / total) if total else 1.0

# ---------------- Schemas default/por layout ----------------
def _schema_oab() -> Dict[str, str]:
    return {
        "nome": "",
        "inscricao": "",
        "seccional": "",
        "subsecao": "",
        "categoria": "",
        "endereco_profissional": "",
        "telefone_profissional": "",
        "situacao": "",
    }

def _validate_user_schema(label: str, user_schema: dict) -> Dict[str, str]:
    """
    Aceita um dict (chaves → ignoramos os valores) e retorna um schema
    {chave: ""} preservando a ordem enviada pelo cliente.
    Valida se as chaves pertencem ao superset do label.
    """
    if not isinstance(user_schema, dict) or not user_schema:
        raise ValueError("extraction_schema inválido: espere um objeto JSON com chaves.")

    if label == "carteira_oab":
        sup = set(_schema_oab().keys())
    else:
        sup = set(_schema_tela_superset().keys())

    bad = [k for k in user_schema.keys() if k not in sup]
    if bad:
        raise ValueError(f"chaves não suportadas para label '{label}': {bad}")

    # preserva a ordem de inserção do cliente
    return {k: "" for k in user_schema.keys()}

def _schema_tela_superset() -> Dict[str, str]:
    return {
        # v3
        "data_referencia": "",
        "selecao_de_parcelas": "",
        "total_de_parcelas": "",
        # v2
        "pesquisa_por": "",
        "pesquisa_tipo": "",
        "sistema": "",
        "valor_parcela": "",
        "cidade": "",
        # v1 (mantém o typo do dataset)
        "data_base": "",
        "data_verncimento": "",
        "quantidade_parcelas": "",
        "produto": "",
        "tipo_de_operacao": "",
        "tipo_de_sistema": "",
    }

def _schema_for_layout(layout: str) -> Dict[str, str]:
    if layout == "v1":
        return {k: "" for k in V1_KEYS_TYPO}  # usa as chaves com o typo
    if layout == "v2":
        return {k: "" for k in V2_KEYS}
    if layout == "v3":
        return {k: "" for k in V3_KEYS}
    return _schema_tela_superset()

# ---------------- Inferência de label/layout ----------------
_OAB_HINTS = (
    r"\bOAB\b", r"inscri", r"seccional", r"subse", r"categoria", r"situa",
    r"endere[cç]o profissional", r"conselho seccional"
)

def _infer_label_from_name(name: str) -> Optional[str]:
    n = (name or "").lower()
    if "oab" in n:
        return "carteira_oab"
    return None

def _infer_label_from_text(lines: list[str]) -> Optional[str]:
    text = "\n".join(lines).lower()
    for h in _OAB_HINTS:
        if re.search(h, text):
            return "carteira_oab"
    return None

def _infer_label(tmp_pdf_path: str, original_name: str) -> str:
    by_name = _infer_label_from_name(original_name)
    if by_name:
        return by_name
    try:
        lines = extract_lines(tmp_pdf_path)[:120]
        by_text = _infer_label_from_text(lines)
        if by_text:
            return by_text
    except Exception:
        pass
    return "tela_sistema"

def _infer_tela_tipo_from_filename(name: str) -> Optional[str]:
    n = (name or "").lower()
    if "detalh" in n or "parcel" in n:
        return "detalhamento_saldos"
    if "consulta" in n or "cobranc" in n:
        return "consulta_cobranca"
    if "operac" in n:
        return "operacao"
    return None

def _detect_layout_by_text(lines: list[str]) -> str:
    """
    Prioriza v1 se houver sinais fortes de cadastro/operacao:
      ('produto' OU 'data base/dt. base/data da base') E
      ('tipo de operação/operacao' OU 'tipo de sistema/sistema')
    Depois v3, depois v2, senão cai no detect do field_matcher.
    """
    text = "\n".join(l.lower() for l in lines)

    has_prod = "produto" in text
    has_data_base = ("data base" in text) or ("dt. base" in text) or ("data da base" in text)
    has_tipo_op = ("tipo operação" in text) or ("tipo de operação" in text) or ("tipo operacao" in text) or ("tipo de operacao" in text)
    has_tipo_sys = ("tipo de sistema" in text) or ("tipo sistema" in text) or ("tipo do sistema" in text)

    if (has_prod or has_data_base) and (has_tipo_op or has_tipo_sys):
        return "v1"

    has_detalhe = "detalhamento de saldos por parcelas" in text
    has_sel = ("seleção de parcelas" in text) or ("selecao de parcelas" in text)
    has_total = ("total geral" in text) or re.search(r"\btotal\b", text) is not None
    if has_detalhe or (has_sel and has_total):
        return "v3"

    has_pesquisa = (("pesquisar por" in text) or ("pesquisa por" in text)) and ("tipo:" in text)
    if has_pesquisa:
        return "v2"

    try:
        return fm_detect_layout(lines)
    except Exception:
        return "v1"

# ---------------- LLM hints (para o fallback) ----------------
def _make_llm_hints(label: str, chosen_layout: Optional[str], lines: list[str]) -> Optional[Dict]:
    """Aplica no layout v1 de 'tela_sistema' para evitar usar filtros do topo."""
    if label != "tela_sistema" or chosen_layout != "v1":
        return None
    return {
        "priority_sections": [
            "Operação Selecionada",
            "Dados Básicos",
            "Resumo",
        ],
        "deprioritize_sections": [
            "Consulta de Cobrança",
            "Pesquisar por",
            "Pesquisa por",
            "Filtro",
            "Tipo:",
        ],
        "field_aliases": {
            "data_base": ["Data Base", "Dt. Base", "Data da Base"],
            "data_verncimento": ["Data Vencimento", "Dt. Vencimento", "Vencimento"],
            "quantidade_parcelas": ["Qtd. Parcelas", "Quantidade de Parcelas"],
            "produto": ["Produto"],
            "sistema": ["Sistema"],
            "tipo_de_operacao": ["Tipo Operação", "Tipo de Operação"],
            "tipo_de_sistema": ["Tipo Sistema", "Tipo de Sistema"],
        },
        "instructions_pt": (
            "Extraia APENAS valores das seções 'Operação Selecionada' e 'Dados Básicos'. "
            "NÃO use valores de filtros do topo ('Consulta de Cobrança', 'Pesquisar por', 'Tipo:', 'Sistema: Contrato'). "
            "Se houver rótulos repetidos, prefira os das seções prioritárias. "
            "Responda exatamente nas chaves do schema fornecido."
        ),
        "full_text": "\n".join(lines) if lines else ""
    }

# ---------------- Endpoints ----------------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

@app.get("/health", tags=["health"])
async def health():
    meta = _llm_meta()
    return {
        "ok": True,
        "status": "healthy",
        "llm": meta,
        "min_coverage": EXTRACT_MIN_COVERAGE,
        "force_layout": API_FORCE_LAYOUT or None,
        "default_use_llm": DEFAULT_USE_LLM,
    }


# caminho absoluto para a pasta static (dentro de app/)
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if not os.path.isdir(STATIC_DIR):
    print(f"[WARN] STATIC_DIR não existe: {STATIC_DIR}")

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

from fastapi.responses import FileResponse

@app.get("/ui", include_in_schema=False)
def ui():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    path = os.path.join(STATIC_DIR, "favicon.ico")
    if os.path.isfile(path):
        return FileResponse(path)
    return JSONResponse({"ok": False, "error": "favicon not found"}, status_code=404)

@app.post("/extract", tags=["extract"])
async def extract_endpoint(
    file: UploadFile = File(..., description="PDF para extração"),
    use_llm: Optional[bool] = Form(None, description="Fallback LLM quando cobertura < limiar"),
    extraction_schema: str = Form(..., description='Use "ALL" para todas as chaves conhecidas ou JSON com subset de chaves a extrair'),
):
    """
    Fluxo:
      1) Inferir label (OAB vs tela)
      2) (Somente tela_sistema) Detectar layout por TEXTO (v1/v2/v3) — com override opcional por env
      3) Definir SCHEMA-ALVO:
         - Se o cliente enviou `extraction_schema`, usar exatamente esse subset (validado).
         - Caso contrário:
            * OAB  -> superset fixo (_schema_oab)
            * Tela -> superset conhecido (_schema_tela_superset)
         (Ou seja: padrão = superset; layout não recorta a resposta.)
      4) Extrair. Se cobertura (medida nas chaves do layout) < limiar e use_llm=True, refaz com LLM.
      5) Responder nas chaves do SCHEMA-ALVO (ordem preservada), sem reordenar por layout.
    """
    # 1) upload -> arquivo temp
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp_path = tmp.name
            shutil.copyfileobj(file.file, tmp)
    except Exception as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": f"falha ao receber o arquivo: {e}"})
    finally:
        try:
            await file.close()
        except Exception:
            pass

    original_name = file.filename or "upload.pdf"

    # 2) ler linhas e inferir label
    try:
        lines = extract_lines(tmp_path)
    except Exception:
        lines = []

    label = _infer_label(tmp_path, original_name)

    # Detecta layout apenas para métrica/fallback quando for tela_sistema
    chosen_layout = None
    if label != "carteira_oab":
        detected = API_FORCE_LAYOUT if API_FORCE_LAYOUT in {"v1", "v2", "v3"} else _detect_layout_by_text(lines)
        chosen_layout = detected

    # 3) schema-alvo (prioridade para o enviado pelo cliente)
    # 3) schema-alvo (obrigatório: "ALL" ou JSON)
    user_schema_provided = False
    try:
        if extraction_schema.strip().upper() == "ALL":
            # superset por label
            if label == "carteira_oab":
                schema = _schema_oab()
            else:
                schema = _schema_tela_superset()
        else:
            # subset enviado pelo cliente (validação contra superset do label)
            user_schema_dict = json.loads(extraction_schema)
            schema = _validate_user_schema(label, user_schema_dict)
            user_schema_provided = True
    except Exception as e:
        try: os.unlink(tmp_path)
        except Exception: pass
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": f"extraction_schema inválido (use 'ALL' ou um JSON de chaves): {e}"}
        )


    # 4) kwargs da 1ª passada — SEM LLM (determinística)
    kwargs = {"use_llm": False}
    if label == "tela_sistema":
        tt = _infer_tela_tipo_from_filename(original_name)
        if tt:
            kwargs["tela_sistema_tipo"] = tt

    # 5) primeira passada
    llm_error: Optional[str] = None
    llm_was_used: bool = False
    try:
        values = run_extract(label, schema, tmp_path, **kwargs)
    except Exception as e:
        try: os.unlink(tmp_path)
        except Exception: pass
        return JSONResponse(status_code=500, content={"ok": False, "error": f"falha na extração: {e}"})

    # 6) cobertura + possível 2ª passada com LLM
    cov_before = None
    cov_final = None
    effective_use_llm = (use_llm if use_llm is not None else DEFAULT_USE_LLM)

    # helper: chaves de layout para métrica (sem recortar saída)
    def _layout_keys(layout: str) -> list[str]:
        if layout == "v1":
            return list(V1_KEYS_TYPO)
        if layout == "v2":
            return list(V2_KEYS)
        if layout == "v3":
            return list(V3_KEYS)
        return list(_schema_tela_superset().keys())

    def _cov_on(keys: list[str], vals: dict) -> float:
        total = max(1, len(keys))
        filled = sum(1 for k in keys if vals.get(k) not in (None, ""))
        return filled / total

    # Métrica de cobertura e retry apenas fazem sentido para tela_sistema
    if label == "tela_sistema" and chosen_layout:
        metric_keys = _layout_keys(chosen_layout)
        cov_before = _cov_on(metric_keys, values)

        if cov_before < EXTRACT_MIN_COVERAGE and effective_use_llm and _llm_env_ok():
            try:
                rk = {k: v for k, v in kwargs.items() if k != "use_llm"}
                values_retry = run_extract(
                    label, schema, tmp_path,
                    use_llm=True,
                    # llm_hints=_make_llm_hints(label, chosen_layout, lines),
                    **rk
                )
                cov_retry = _cov_on(metric_keys, values_retry)
                if cov_retry > cov_before:
                    values = values_retry
                    cov_before = cov_retry
                llm_was_used = True
            except Exception as e:
                llm_error = str(e)

        # Importante: a RESPOSTA segue o schema-alvo (superset ou subset do cliente), sem reordenar por layout
        values = {k: (values.get(k) or "") for k in schema.keys()}
        cov_final = _cov_on(metric_keys, values)

    else:
        # OAB (ou tela sem layout detectado): apenas alinhe à máscara do schema-alvo
        values = {k: (values.get(k) or "") for k in schema.keys()}

    # limpar tmp
    try: os.unlink(tmp_path)
    except Exception: pass

    # 7) resposta
    payload = {
        "ok": True,
        "error": None,
        "label": label,
        "extraction_schema": values,
        "pdf_path": original_name,
    }
    if API_DEBUG and label == "tela_sistema":
        payload["debug"] = {
            "layout": chosen_layout,
            "coverage": {
                "threshold": EXTRACT_MIN_COVERAGE,
                "before": cov_before,
                "final": cov_final,
            },
            "llm_requested": bool(effective_use_llm) and bool(_llm_env_ok()) and llm_was_used,
            "llm_error": llm_error,
            "llm": _llm_meta(),
        }
    return JSONResponse(status_code=200, content=payload)

