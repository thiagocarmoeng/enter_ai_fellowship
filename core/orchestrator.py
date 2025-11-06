# core/orchestrator.py
from __future__ import annotations

import os
from typing import Dict, Optional, Any

from .pdf_reader import extract_lines
from .field_matcher import extract as fm_extract  # heurística principal (best-fit)
from .validators import normalize_all
from .llm_client import LLMClient
from . import cache as _cache

# -----------------------------------------------------------------------------
# Config / Aliases
# -----------------------------------------------------------------------------
SUPPORTED_LABELS = {"carteira_oab", "tela_sistema"}

# aliases por label (para typos - chave canônica)
ALIASES: Dict[str, Dict[str, str]] = {
    "tela_sistema": {
        "data_verncimento": "data_vencimento", 
    },
    "carteira_oab": {
        # ex.: "uf": "seccional"
    },
}

STRICT_LABELS = False  # True - bloqueia labels fora de SUPPORTED_LABELS
VERBOSE = os.getenv("ORCH_VERBOSE", "0") == "1"
MIN_COVERAGE = float(os.getenv("EXTRACT_MIN_COVERAGE", "0.9"))  # 0.90 = 90%
LLM_MAX_FIELDS = int(os.getenv("LLM_MAX_FIELDS", "99"))  

# âncoras para montar contexto do LLM
ANCHORS_BY_LABEL = {
    "carteira_oab": [
        "inscri", "seccional", "subse", "categoria", "telefone", "situa",
        "endereço profissional", "endereco profissional"
    ],
    "tela_sistema": [
        "Data Base", "Venc", "Qtd", "Produto", "Sistema", "Tipo Operação",
        "Tipo de Operação", "Tipo Sistema", "Pesquisar por", "Pesquisa por",
        "Tipo:", "Cidade", "Detalhamento de saldos por parcelas",
        "Seleção de parcelas", "Selecao de parcelas", "Total Geral", "Total"
    ],
}

# conjuntos de chaves por layout Conehcido
V1_KEYS = ("data_base", "data_vencimento", "quantidade_parcelas",
           "produto", "sistema", "tipo_de_operacao", "tipo_de_sistema")
V2_KEYS = ("pesquisa_por", "pesquisa_tipo", "sistema", "valor_parcela", "cidade")
V3_KEYS = ("data_referencia", "selecao_de_parcelas", "total_de_parcelas")


# -----------------------------------------------------------------------------
# Utils internos
# -----------------------------------------------------------------------------
def _vprint(msg: str) -> None:
    if VERBOSE:
        print(f"[orchestrator] {msg}")

def _context_from_anchors(
    lines: list[str],
    label: str,
    missing_keys: list[str],
    win: int = 2,
    max_frag: int = 12,
    max_chars: int = 3000,
) -> str:
    """Recorta janelas de texto em torno de âncoras relevantes às chaves faltantes."""
    keys = [k.lower() for k in missing_keys]
    anchors = ANCHORS_BY_LABEL.get(label, [])
    prefer = [a for a in anchors if any(k in a.lower() or a.lower() in k for k in keys)]
    picked: list[str] = []
    for i, ln in enumerate(lines):
        low = ln.lower()
        if any(a.lower() in low for a in (prefer or anchors)):
            frag = "\n".join(lines[max(0, i - win): min(len(lines), i + 1 + win)]).strip()
            if frag:
                picked.append(frag)
            if len(picked) >= max_frag:
                break
    if not picked:
        picked = ["\n".join(lines[:10])] 
    out, size = [], 0
    for frag in picked:
        if size + len(frag) > max_chars:
            break
        out.append(frag)
        size += len(frag)
    return "\n---\n".join(out)

def _canonicalize_schema(label: str, schema: Dict[str, str]) -> Dict[str, str]:
    """Aplica aliases e deduplica mantendo a primeira ocorrência."""
    mapping = ALIASES.get(label, {})
    canon_schema: Dict[str, str] = {}
    for orig_k, desc in (schema or {}).items():
        canon_k = mapping.get(orig_k, orig_k)
        if canon_k not in canon_schema:
            canon_schema[canon_k] = desc
    return canon_schema

def _map_back_to_original(
    label: str,
    original_schema: Dict[str, str],
    data_canon: Dict[str, Optional[str]],
) -> Dict[str, Optional[str]]:
    """Volta a usar exatamente as chaves pedidas (inclui typos)."""
    mapping = ALIASES.get(label, {})
    out: Dict[str, Optional[str]] = {}
    for orig_key in original_schema.keys():
        canon_key = mapping.get(orig_key, orig_key)
        out[orig_key] = data_canon.get(canon_key)
    return out

def _compute_cache_key(
    pdf_path: str,
    label: str,
    canon_schema: Dict[str, str],
    tela_tipo: Optional[str] = None,
) -> Optional[str]:
    """hash(pdf) + label + conjunto de chaves [+ tipo de tela]."""
    try:
        keys_sorted = "|".join(sorted(canon_schema.keys()))
        tipo_part = f"::{tela_tipo}" if tela_tipo else ""
        return f"{_cache.file_hash(pdf_path)}::{label}::{keys_sorted}{tipo_part}"
    except Exception:
        return None

def _empty_result(schema: Dict[str, str]) -> Dict[str, Optional[str]]:
    return {k: None for k in (schema or {}).keys()}

def _is_subset(keys: list[str], target: tuple[str, ...]) -> bool:
    return set(keys).issubset(set(target))


# -----------------------------------------------------------------------------
# Orquestrador
# -----------------------------------------------------------------------------
def run_extract(
    label: str,
    schema: Dict[str, str],
    pdf_path: str,
    use_llm: bool = False,
    llm_hints: Optional[Dict[str, Any]] = None,  # -  novo: dicas para o LLM (seções/aliases/instruções)
    **meta: Any,  # ex.: tela_sistema_tipo="operacao|consulta_cobranca|detalhamento_saldos"
) -> Dict[str, Optional[str]]:
    """
    1) Canoniza chaves + (cache somente para chamadas sem LLM)
    2) Extrai via heurística (field_matcher - best-fit por layout)
    3) Normaliza (validators)
    4) Se cobertura < MIN_COVERAGE e use_llm=True - LLM preenche faltantes (com hints opcionais)
    5) Normaliza novamente, decide cache e retorna mapeando para as chaves originais
    """

    # ------------------- helpers locais -------------------
    def _ctx_with_hints(
        lines: list[str],
        label_: str,
        missing: list[str],
        win: int = 2,
        max_frag: int = 10,
        max_chars: int = 2500,
        hints: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Monta contexto priorizando seções (quando hints existem)."""
        if not lines:
            return ""
        if not hints:
            return _context_from_anchors(lines, label_, missing, win=win, max_frag=max_frag, max_chars=max_chars)

        pri = [s.lower() for s in hints.get("priority_sections", [])]
        dep = [s.lower() for s in hints.get("deprioritize_sections", [])]

        scored: list[tuple[int, int]] = []
        for i, ln in enumerate(lines):
            l = ln.lower()
            score = 0
            if any(p in l for p in pri):
                score += 5
            if any(d in l for d in dep):
                score -= 5
            scored.append((score, i))

        scored.sort(reverse=True)  # maiores scores primeiro
        frags, used = [], set()
        for score, i in scored:
            if len(frags) >= max_frag:
                break
            if score <= 0 and frags:
                break
            a, b = max(0, i - win), min(len(lines), i + 1 + win)
            if (a, b) in used:
                continue
            frag = "\n".join(lines[a:b]).strip()
            if frag:
                frags.append(frag)
                used.add((a, b))

        if not frags:
            return _context_from_anchors(lines, label_, missing, win=win, max_frag=max_frag, max_chars=max_chars)

        ctx = "\n---\n".join(frags)
        return ctx[:max_chars]

    # ------------------- validações -------------------
    if not isinstance(schema, dict) or not schema:
        _vprint("schema vazio ou inválido; retornando empty_result")
        return _empty_result(schema or {})
    if not isinstance(label, str) or not label:
        _vprint("label vazio/ausente; retornando empty_result")
        return _empty_result(schema)
    if STRICT_LABELS and label not in SUPPORTED_LABELS:
        _vprint(f"label '{label}' não suportado (STRICT_LABELS=True)")
        return _empty_result(schema)
    if not isinstance(pdf_path, str) or not pdf_path or not os.path.exists(pdf_path):
        _vprint(f"pdf_path inválido/ausente: {pdf_path!r}")
        return _empty_result(schema)

    # ------------------- canonização -------------------
    canon_schema = _canonicalize_schema(label, schema)
    _vprint(f"canon_schema keys={list(canon_schema.keys())}")

    # tipo de tela (só para granular cache key)
    tela_tipo = meta.get("tela_sistema_tipo") if label == "tela_sistema" else None

    # ------------------- cache lookup -------------------
    cache_key = _compute_cache_key(pdf_path, label, canon_schema, tela_tipo)
    # Não usar cache quando use_llm=True (2ª passada quer forçar nova extração)
    if cache_key and not use_llm:
        cached = _cache.get(cache_key)
        if cached is not None:
            _vprint("cache HIT")
            return _map_back_to_original(label, schema, cached)
        _vprint("cache MISS")

    # ------------------- leitura do PDF -------------------
    try:
        lines = extract_lines(pdf_path)
    except Exception as e:
        _vprint(f"falha extract_lines: {e}")
        return _empty_result(schema)
    if not lines:
        _vprint("nenhuma linha útil no PDF")
        return _empty_result(schema)

    # ------------------- extração heurística -------------------
    try:
        raw = fm_extract(label, canon_schema, lines)
    except Exception as e:
        _vprint(f"falha field_matcher.extract: {e}")
        return _empty_result(schema)

    # somente as chaves do schema canônico
    data_canon: Dict[str, Optional[str]] = {k: raw.get(k) for k in canon_schema.keys()}

    # ------------------- normalização inicial -------------------
    try:
        data_canon = normalize_all(data_canon)
    except Exception as e:
        _vprint(f"normalize_all falhou (mantendo brutos): {e}")

    # ------------------- cobertura / layout -------------------
    canon_keys = list(canon_schema.keys())
    keys_expected = canon_keys 
    chosen_layout = None

    if label == "tela_sistema":
        if _is_subset(canon_keys, V1_KEYS):
            keys_expected = list(V1_KEYS)
            chosen_layout = "v1"
        elif _is_subset(canon_keys, V2_KEYS):
            keys_expected = list(V2_KEYS)
            chosen_layout = "v2"
        elif _is_subset(canon_keys, V3_KEYS):
            keys_expected = list(V3_KEYS)
            chosen_layout = "v3"
        else:
            # Superset/ambíguo - escolhe layout pelo melhor preenchimento bruto
            from .field_matcher import extract_tela_v1, extract_tela_v2, extract_tela_v3
            c1 = extract_tela_v1([k for k in canon_keys if k in V1_KEYS], lines)
            c2 = extract_tela_v2([k for k in canon_keys if k in V2_KEYS], lines)
            c3 = extract_tela_v3([k for k in canon_keys if k in V3_KEYS], lines)
            s1 = sum(1 for k, v in c1.items() if v not in (None, ""))
            s2 = sum(1 for k, v in c2.items() if v not in (None, ""))
            s3 = sum(1 for k, v in c3.items() if v not in (None, ""))
            ranked = sorted(
                [("v1", s1, V1_KEYS), ("v2", s2, V2_KEYS), ("v3", s3, V3_KEYS)],
                key=lambda t: (t[1], {"v1": 1, "v2": 2, "v3": 3}[t[0]]),
                reverse=True,
            )
            chosen_layout = ranked[0][0]
            keys_expected = [k for k in canon_keys if k in ranked[0][2]]
    else:
        # carteira_oab: usa todas as chaves do schema
        keys_expected = canon_keys

    filled_keys = sum(1 for k in keys_expected if data_canon.get(k) not in (None, ""))
    total_keys = len(keys_expected) if keys_expected else len(canon_keys)
    coverage = (filled_keys / total_keys) if total_keys else 1.0
    _vprint(
        f"coverage={coverage:.2%} ({filled_keys}/{total_keys})"
        f"{' [layout='+chosen_layout+']' if chosen_layout else ''}"
    )

    # ------------------- LLM fallback -------------------
    if use_llm and coverage < MIN_COVERAGE:
        try:
            missing_keys = [k for k in keys_expected if (data_canon.get(k) in (None, ""))]

            # pular chaves “ruins”
            SKIP_LLM_FOR = {
                "carteira_oab": {"telefone_profissional"},
                "tela_sistema": set(),
            }
            missing_keys = [k for k in missing_keys if k not in SKIP_LLM_FOR.get(label, set())]
            missing_keys.sort() 

            if missing_keys and len(missing_keys) <= LLM_MAX_FIELDS:
                _vprint(f"LLM fallback ativado: missing_keys={missing_keys}")
                context = _ctx_with_hints(
                    lines, label, missing_keys, win=2, max_frag=10, max_chars=2500, hints=llm_hints
                )
                client = LLMClient()
                extra_kwargs: Dict[str, Any] = {}
                if llm_hints:
                    extra_kwargs["instructions_pt"] = llm_hints.get("instructions_pt")
                    extra_kwargs["field_aliases"] = llm_hints.get("field_aliases")
                    
                    extra_kwargs["full_text"] = llm_hints.get("full_text")

                llm_out = client.solve(
                    label=label,
                    schema_keys=list(canon_schema.keys()),
                    missing_keys=missing_keys,
                    context=context,
                    **extra_kwargs,
                )
                for k in missing_keys:
                    if llm_out.get(k):
                        data_canon[k] = llm_out[k]

                filled_keys = sum(1 for k in keys_expected if data_canon.get(k) not in (None, ""))
                coverage = (filled_keys / total_keys) if total_keys else 1.0
                _vprint(f"coverage pós-LLM={coverage:.2%}")
            else:
                _vprint(f"LLM pulado (missing={len(missing_keys)})")
        except Exception as e:
            _vprint(f"LLM indisponível/erro: {e}")

    # ------------------- normalização final -------------------
    try:
        data_canon = normalize_all(data_canon)
    except Exception:
        pass

    # ------------------- decisão de cache -------------------
    # Recalcula cobertura final com as mesmas chaves esperadas
    filled_keys = sum(1 for k in keys_expected if data_canon.get(k) not in (None, ""))
    total_keys = len(keys_expected) if keys_expected else len(canon_keys)
    coverage_final = (filled_keys / total_keys) if total_keys else 1.0

    if cache_key:
        try:
            # Grava cache apenas se:
            #  - cobertura final >= MIN_COVERAGE (resultado confiável), OU
            #  - esta chamada usou LLM (resultado já “reforçado”).
            if (coverage_final >= MIN_COVERAGE) or use_llm:
                _cache.set_(cache_key, data_canon)
                _vprint("cache SET")
            else:
                _vprint("cache SKIP (coverage baixa sem LLM)")
        except Exception as e:
            _vprint(f"falha ao setar cache: {e}")

    # ------------------- retorno -------------------
    return _map_back_to_original(label, schema, data_canon)

