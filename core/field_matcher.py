# core/field_matcher.py
from __future__ import annotations
import re
from typing import Dict, List, Optional
from .layout_indexer import neighborhood

# --- no topo do arquivo (perto das defs de regex) ---
V1_KEYS = ("data_base", "data_verncimento", "quantidade_parcelas",
           "produto", "sistema", "tipo_de_operacao", "tipo_de_sistema")
V2_KEYS = ("pesquisa_por", "pesquisa_tipo", "sistema", "valor_parcela", "cidade")
V3_KEYS = ("data_referencia", "selecao_de_parcelas", "total_de_parcelas")


# ===== Regex utilitárias ======================================================
RE_UF       = re.compile(r"\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MG|MS|MT|PA|PB|PE|PI|PR|RJ|RN|RO|RR|RS|SC|SE|SP|TO)\b")
RE_PHONE    = re.compile(r"\(?\d{2}\)?\s?\d{4,5}-?\d{4}")
RE_INT_1_3  = re.compile(r"\b\d{1,3}\b")
RE_INT_5_7  = re.compile(r"\b\d{5,7}\b")
RE_DATE     = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")
RE_MONEY    = re.compile(r"\b\d{1,3}(?:\.\d{3})*,\d{2}\b", re.I)

# ===== Helpers genéricos ======================================================
def lines_between(lines: List[str], start_idx: int, stop_anchors: List[str], max_ahead: int = 8) -> List[str]:
    """Coleta linhas após start_idx até encontrar um dos rótulos de parada ou atingir max_ahead."""
    collected = []
    for j in range(start_idx + 1, min(len(lines), start_idx + 1 + max_ahead)):
        low = lines[j].lower()
        if any(a in low for a in stop_anchors):
            break
        text = lines[j].strip()
        if text:
            collected.append(text)
    return collected

def value_after_label(line: str, pattern: re.Pattern, next_line: Optional[str] = None) -> Optional[str]:
    """Prioriza valor após ':'; se não achar, procura na linha inteira; se ainda não, tenta a próxima linha."""
    def _try(s: str) -> Optional[str]:
        if ":" in s:
            tail = s.split(":", 1)[1]
            m = pattern.search(tail)
            if m:
                return m.group(0)
        m = pattern.search(s)
        return m.group(0) if m else None

    v = _try(line)
    if v:
        return v
    if next_line:
        return _try(next_line)
    return None

def find_line(lines: List[str], *anchors: str) -> Optional[int]:
    for i, ln in enumerate(lines):
        low = ln.lower()
        if any(a.lower() in low for a in anchors):
            return i
    return None

# =============================================================================
#                                    OAB
# =============================================================================
def _clean_subsecao(val: str, inscricao: Optional[str]) -> Optional[str]:
    if not val:
        return None
    s = re.sub(r"\bCEP\b.*$", "", val, flags=re.I).strip(" :;|-")
    # rejeita somente números (especialmente 5–7 dígitos)
    if RE_INT_5_7.fullmatch(s) or s.isdigit():
        return None
    # se for igual à inscrição (ignorando não-dígitos), rejeita
    if inscricao and re.sub(r"\D", "", s) == re.sub(r"\D", "", inscricao):
        return None
    # exige pelo menos uma letra
    if not any(ch.isalpha() for ch in s):
        return None
    return re.sub(r"\s+", " ", s).strip()


def extract_oab(keys: List[str], lines: List[str]) -> Dict[str, Optional[str]]:
    out = {k: None for k in keys}

    # Nome — normalmente nas 5–6 primeiras linhas
    if "nome" in out:
        for ln in lines[:6]:
            s = ln.strip()
            if any(ch.isalpha() for ch in s) and len(s) >= 3:
                out["nome"] = s
                break

    stop_after_endereco = [
        "telefone profissional", "situa", "categoria",
        "seccional", "subse", "conselho seccional"
    ]

    insc_val: Optional[str] = None  # <<< novo: guardar inscrição para checar subseção

    for i, ln in enumerate(lines):
        low = ln.lower()

        # Inscrição
        if "inscricao" in out and out.get("inscricao") is None and "inscri" in low:
            v = value_after_label(
                ln, RE_INT_5_7,
                " ".join(neighborhood(lines, i, 1)) if i + 1 < len(lines) else None
            ) or value_after_label(" ".join(neighborhood(lines, i, 2)), RE_INT_5_7)
            if v:
                out["inscricao"] = v
                insc_val = v  # <<< novo

        # Seccional (UF)
        if "seccional" in out and out.get("seccional") is None and ("seccional" in low or "conselho seccional" in low):
            v = value_after_label(
                ln, RE_UF,
                " ".join(neighborhood(lines, i, 1)) if i + 1 < len(lines) else None
            ) or value_after_label(" ".join(neighborhood(lines, i, 2)), RE_UF)
            if v:
                out["seccional"] = v

        # ---------------------- Subseção (ajustado) ----------------------
        if "subsecao" in out and out.get("subsecao") is None and ("subse" in low or "conselho seccional" in low):
            # 1) tenta explicitamente "Subseção: <texto>"
            tail = ln.split(":", 1)[1] if ":" in ln else " ".join(neighborhood(lines, i, 1))
            m = re.search(r"Subse(?:ç|c)[aã]o[:\s-]*([\wÀ-ÿ \-]+)", tail, re.I)
            cand = m.group(1).strip() if m else ""
            val = _clean_subsecao(cand, insc_val or out.get("inscricao"))

            # 2) se vier no formato "CONSELHO SECCIONAL - PARANÁ" na própria linha
            if not val and "conselho seccional" in low:
                m2 = re.search(r"(CONSELHO\s+SECCIONAL\s*-\s*[\wÀ-ÿ \-]+)", ln, re.I)
                if m2:
                    val = _clean_subsecao(m2.group(1), insc_val or out.get("inscricao"))

            # 3) se a linha atual só tem o rótulo, olha a próxima
            if not val and i + 1 < len(lines):
                val = _clean_subsecao(lines[i+1], insc_val or out.get("inscricao"))

            if val:
                out["subsecao"] = val
        # ----------------------------------------------------------------

        # Categoria
        if "categoria" in out and out.get("categoria") is None:
            if any(w in low for w in ("suplementar", "advog", "estagi")):
                out["categoria"] = re.sub(r"\s+", " ", ln.strip().upper())

        # Endereço profissional
        if "endereco_profissional" in out and out.get("endereco_profissional") is None and (
            "endereço profissional" in low or "endereco profissional" in low
        ):
            tail = ln.split(":", 1)[1].strip() if ":" in ln else ""
            chunk = lines_between(lines, i, stop_after_endereco, max_ahead=6)
            joined = " ".join([tail] + chunk).strip() if tail else " ".join(chunk).strip()
            joined_clean = re.split(r"\bCEP\b|\bBrasil\b", joined, 1)[0].strip() or joined
            out["endereco_profissional"] = joined_clean or None

        # Telefone
        if "telefone_profissional" in out and out.get("telefone_profissional") is None and "telefone" in low:
            nxt = lines[i+1] if i + 1 < len(lines) else None
            v = value_after_label(ln, RE_PHONE, nxt) or value_after_label(" ".join(neighborhood(lines, i, 3)), RE_PHONE)
            if v:
                out["telefone_profissional"] = v

        # Situação
        if "situacao" in out and out.get("situacao") is None and "situa" in low:
            out["situacao"] = ln.strip().upper()

    # Fallbacks “suaves”
    all_text = " ".join(lines)
    if "inscricao" in out and out["inscricao"] is None:
        m = RE_INT_5_7.search(all_text)
        if m:
            out["inscricao"] = m.group(0)
            insc_val = out["inscricao"]

    if "telefone_profissional" in out and out["telefone_profissional"] is None:
        m = RE_PHONE.search(all_text)
        if m:
            out["telefone_profissional"] = m.group(0)

    # Checagem final: se subseção acabou numérico/igual à inscrição → descarta
    if "subsecao" in out and out["subsecao"] and out.get("inscricao"):
        if re.sub(r"\D", "", out["subsecao"]) == re.sub(r"\D", "", out["inscricao"]):
            out["subsecao"] = None

    return out

# =============================================================================
#                              TELAS DE SISTEMA
# =============================================================================
def has_any(line: str, *needles: str) -> bool:
    low = line.lower()
    return any(n.lower() in low for n in needles)

def _coverage_score(vals: Dict[str, Optional[str]], keys: List[str]) -> int:
    return sum(1 for k in keys if (vals.get(k) not in (None, "")))

def _extract_candidates(keys: List[str], lines: List[str]):
    return [
        extract_tela_v1(keys, lines),
        extract_tela_v2(keys, lines),
        extract_tela_v3(keys, lines),
    ]

# --- no topo do arquivo (perto das defs de regex) ---
V1_KEYS = ("data_base", "data_verncimento", "quantidade_parcelas",
           "produto", "sistema", "tipo_de_operacao", "tipo_de_sistema")
V2_KEYS = ("pesquisa_por", "pesquisa_tipo", "sistema", "valor_parcela", "cidade")
V3_KEYS = ("data_referencia", "selecao_de_parcelas", "total_de_parcelas")

def _coverage_score(vals: Dict[str, Optional[str]], keys: List[str]) -> int:
    return sum(1 for k in keys if (vals.get(k) not in (None, "")))

def _extract_candidates(keys: List[str], lines: List[str]):
    return [
        extract_tela_v1(keys, lines),
        extract_tela_v2(keys, lines),
        extract_tela_v3(keys, lines),
    ]

def extract_tela(keys: List[str], lines: List[str]) -> Dict[str, Optional[str]]:
    ks = set(keys)
    if ks and ks.issubset(V1_KEYS):
        return extract_tela_v1(keys, lines)
    if ks and ks.issubset(V2_KEYS):
        return extract_tela_v2(keys, lines)
    if ks and ks.issubset(V3_KEYS):
        return extract_tela_v3(keys, lines)

    candidates = _extract_candidates(keys, lines)
    scores = [_coverage_score(cand, keys) for cand in candidates]
    # desempate previsível: v3 > v2 > v1
    priorities = [1, 2, 3]  # v1=1, v2=2, v3=3
    best_idx = max(range(3), key=lambda i: (scores[i], priorities[i]))
    return {k: candidates[best_idx].get(k) for k in keys}




def detect_tela_layout(lines: List[str]) -> str:
    text = "\n".join(l.lower() for l in lines)

    has_tipo_op  = ("tipo operação" in text) or ("tipo de operação" in text)
    has_tipo_sis = ("tipo sistema" in text) or ("tipo de sistema" in text) or ("tipo do sistema" in text)
    has_pesquisa = (("pesquisar por" in text) or ("pesquisa por" in text)) and ("tipo:" in text)

    has_detalhe  = ("detalhamento de saldos por parcelas" in text)
    has_sel      = ("seleção de parcelas" in text) or ("selecao de parcelas" in text)
    has_total    = ("total geral" in text) or bool(re.search(r"\btotal\b", text))

    # v3 se tiver o título OU (seleção de parcelas e algum 'total')
    if has_detalhe or (has_sel and has_total):
        return "v3"
    if has_pesquisa:
        return "v2"
    if has_tipo_op and has_tipo_sis:
        return "v1"
    # fallback: se há seleção/total, tende a ser v3
    if has_sel or has_total:
        return "v3"
    return "v1"

def find_anchor_with_context(lines: List[str], *anchors: str) -> Optional[tuple[int, str]]:
    """Retorna (idx, anchor_match) da primeira linha que contenha alguma âncora."""
    for i, ln in enumerate(lines):
        low = ln.lower()
        for a in anchors:
            if a.lower() in low:
                return i, a
    return None

def first_group_or_none(pattern: str, text: str, flags=re.I) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None

def _after_anchor(line: str, prefer: str = "date|money|int") -> Optional[str]:
    """Extrai valor preferindo date/money/int na ordem indicada (somente na linha)."""
    order = []
    if "date" in prefer:  order.append(RE_DATE)
    if "money" in prefer: order.append(RE_MONEY)
    if "int" in prefer:   order.append(RE_INT_1_3)
    for rx in order:
        v = value_after_label(line, rx)
        if v:
            return v
    return None

# -------------------- Extrator v1 (cadastro/consulta) ------------------------
def extract_tela_v1(keys: List[str], lines: List[str]) -> Dict[str, Optional[str]]:
    out = {k: None for k in keys}
    text = "\n".join(lines)

    # ---------- helpers locais ----------
    def _first_date_from_doc(n: int = 1) -> Optional[str]:
        ds = RE_DATE.findall(text)
        if not ds:
            return None
        if n == 1 and len(ds) >= 1:
            return ds[0]
        if n == 2 and len(ds) >= 2:
            return ds[1]
        return ds[-1]

    def _cut_until_next_label(s: str) -> str:
        """
        Corta a string no primeiro encontro de um dos próximos rótulos.
        Evita que 'Produto' ou 'Sistema' capturem coisas como '| Tipo ...'.
        """
        STOP = r"(?:\bProduto\b|\bSistema\b|\bTipo\s+(?:de\s+)?Oper[açc][aã]o\b|\bTipo\s+(?:do\s+)?Sistema\b|\bCidade\b|$)"
        m = re.search(rf"^(.*?)(?=\s*{STOP})", s, re.I)
        return (m.group(1) if m else s).strip(" |-")

    # ---------- datas ----------
    if "data_base" in out:
        i = find_line(lines, "Data Base", "Dt. Base", "Base", "Data da Base", "Data Referência", "Data Referencia")
        if i is not None:
            nxt = lines[i+1] if i+1 < len(lines) else None
            out["data_base"] = value_after_label(lines[i], RE_DATE, nxt)
        if not out["data_base"]:
            out["data_base"] = _first_date_from_doc(1)

    if "data_vencimento" in out:
        i = find_line(lines, "Data Vencimento", "Dt. Venc.", "Vcto", "Vencimento")
        if i is not None:
            nxt = lines[i+1] if i+1 < len(lines) else None
            out["data_vencimento"] = value_after_label(lines[i], RE_DATE, nxt)
        if not out["data_vencimento"]:
            out["data_vencimento"] = _first_date_from_doc(2)

    # ---------- quantidade de parcelas ----------
    if "quantidade_parcelas" in out:
        i = find_line(lines, "Qtd. Parcelas", "Qtd Parc", "Qtd. Parc", "Qtd parcelas",
                      "Quantidade de parcelas", "Qtde de parcelas", "Qtde")
        if i is not None:
            nxt = lines[i+1] if i+1 < len(lines) else None
            out["quantidade_parcelas"] = value_after_label(lines[i], RE_INT_1_3, nxt)
        if not out["quantidade_parcelas"]:
            m = re.search(r"Qt(?:d(?:e|\.)?)?\s*(?:Parc(?:elas)?)?[:\s]*([0-9]{1,3})", text, re.I)
            if m:
                out["quantidade_parcelas"] = m.group(1)

    # ---------- produto ----------
    if "produto" in out:
        i = find_line(lines, "Produto")
        if i is not None:
            v = first_group_or_none(r"Produto[:\s]*(.+)$", lines[i])
            if not v and i+1 < len(lines):
                v = first_group_or_none(r"^([A-Za-zÀ-ÿ0-9/ \-]{3,})", lines[i+1])
            if v:
                out["produto"] = _cut_until_next_label(v)

    # ---------- sistema ----------
    if "sistema" in out:
        i = find_line(lines, "Sistema")
        if i is not None:
            v = first_group_or_none(r"Sistema[:\s]*(.+)$", lines[i])
            if not v and i+1 < len(lines):
                v = first_group_or_none(r"^([A-Za-zÀ-ÿ0-9/ \-]{2,})", lines[i+1])
            if v:
                out["sistema"] = _cut_until_next_label(v)

    # ---------- tipo_de_operacao ----------
    if "tipo_de_operacao" in out:
        i = find_line(lines, "Tipo Operação", "Tipo de Operação", "Tipo Operaçao", "Tipo Operacao")
        if i is not None:
            m = re.search(r"Tipo\s+Oper[açc][aã]o[:\s]*(.+?)(?=\s*Tipo\s+(?:do\s+)?Sistema|$)", lines[i], re.I)
            if m:
                out["tipo_de_operacao"] = m.group(1).strip(" |-")
            elif i+1 < len(lines):
                out["tipo_de_operacao"] = lines[i+1].strip(" |-")

    # ---------- tipo_de_sistema ----------
    if "tipo_de_sistema" in out:
        i = find_line(lines, "Tipo Sistema", "Tipo de Sistema", "Tipo do Sistema")
        if i is not None:
            m = re.search(r"Tipo\s+(?:do\s+)?Sistema[:\s]*(.+)$", lines[i], re.I)
            if m:
                out["tipo_de_sistema"] = m.group(1).strip(" |-")
            elif i+1 < len(lines):
                out["tipo_de_sistema"] = lines[i+1].strip(" |-")

    # ---------- campos v3 presentes por engano (defensivo) ----------
    if "selecao_de_parcelas" in out and out["selecao_de_parcelas"] is None:
        i = find_line(lines, "Seleção de parcelas", "Selecao de parcelas")
        if i is not None:
            win = " ".join(lines[max(0, i-1): min(len(lines), i+3)])
            m = re.search(r"(Vencidas|Vencidos|Pagas?|Pendentes?)", win, re.I)
            if m:
                out["selecao_de_parcelas"] = m.group(1).capitalize()

    if "total_de_parcelas" in out and out["total_de_parcelas"] is None:
        for ln in lines:
            if "total" in ln.lower():
                m = RE_MONEY.search(ln)
                if m:
                    out["total_de_parcelas"] = m.group(0)
                    break

    return out

# -------------------- Extrator v2 (filtro/pesquisa) --------------------------
def extract_tela_v2(keys: List[str], lines: List[str]) -> Dict[str, Optional[str]]:
    out = {k: None for k in keys}

    # pesquisa_por
    if "pesquisa_por" in out:
        i = find_line(lines, "Pesquisar por", "Pesquisa por")
        if i is not None:
            m = re.search(r"Pesquisar?\s+por[:\s]*([A-Za-zÀ-ÿ/ ]+)", lines[i], re.I)
            if not m and i + 1 < len(lines):
                m = re.search(r"^([A-Za-zÀ-ÿ/ ]+)", lines[i + 1], re.I)
            if m:
                out["pesquisa_por"] = m.group(1).strip()

    # pesquisa_tipo
    if "pesquisa_tipo" in out:
        i = find_line(lines, "Tipo:")
        if i is not None:
            m = re.search(r"Tipo:\s*([A-Za-zÀ-ÿ/ ]+)", lines[i], re.I)
            if not m and i + 1 < len(lines):
                m = re.search(r"^([A-Za-zÀ-ÿ/ ]+)", lines[i + 1], re.I)
            if m:
                out["pesquisa_tipo"] = m.group(1).strip()

    # valor parcela (se existir nessa tela)
    if "valor_parcela" in out:
        i = find_line(lines, "Vlr. Parc.", "VIr. Parc.", "Valor Parcela")
        if i is not None:
            nxt = lines[i+1] if i+1 < len(lines) else None
            out["valor_parcela"] = value_after_label(lines[i], RE_MONEY, nxt)

    # cidade
    if "cidade" in out:
        i = find_line(lines, "Cidade")
        if i is not None:
            m = re.search(r"Cidade[:\s]*([A-Za-zÀ-ÿ\s\-]+(?:\s\([A-Z]{2}\))?)\b", lines[i])
            if not m and i + 1 < len(lines):
                m = re.search(r"^([A-Za-zÀ-ÿ\s\-]+(?:\s\([A-Z]{2}\))?)\b", lines[i + 1])
            if m:
                out["cidade"] = m.group(1).strip()

    # sistema (se existir nessa tela)
    if "sistema" in out and out["sistema"] is None:
        i = find_line(lines, "Sistema")
        if i is not None:
            m = re.search(r"Sistema[:\s]*([A-Za-zÀ-ÿ]+)", lines[i])
            if not m and i + 1 < len(lines):
                m = re.search(r"^([A-Za-zÀ-ÿ]+)", lines[i + 1])
            if m:
                out["sistema"] = m.group(1).strip()

    return out

# -------------------- Extrator v3 (detalhamento/total) -----------------------
def extract_tela_v3(keys: List[str], lines: List[str]) -> Dict[str, Optional[str]]:
    out = {k: None for k in keys}
    text = "\n".join(lines)

    # -------- Data de referência --------
    if "data_referencia" in out:
        # 1) genérico: acha linha com "refer" e captura data na mesma linha ou próxima
        got = None
        for i, ln in enumerate(lines):
            low = ln.lower()
            if "refer" in low:  # cobre "referência" / "referencia" / "ref."
                nxt = lines[i+1] if i+1 < len(lines) else None
                v = value_after_label(ln, RE_DATE, nxt)
                if v:
                    got = v
                    break
        # 2) fallback: primeira data do documento
        if not got:
            m = RE_DATE.search(text)
            if m:
                got = m.group(0)
        out["data_referencia"] = got

    # -------- Seleção de parcelas --------
    if "selecao_de_parcelas" in out:
        got = None
        i = find_line(lines, "Seleção de parcelas", "Selecao de parcelas")
        if i is not None:
            win = " ".join(lines[max(0, i-1): min(len(lines), i+3)])
            m = re.search(r"(Vencidas|Vencidos|Pagas?|Pendentes?)", win, re.I)
            if m:
                got = m.group(1).capitalize()
        if not got:
            # fallback: procura termos em todo o texto
            m = re.search(r"\b(Vencidas|Vencidos|Pagas?|Pendentes?)\b", text, re.I)
            if m:
                got = m.group(1).capitalize()
        out["selecao_de_parcelas"] = got

    # -------- Total de parcelas --------
    if "total_de_parcelas" in out:
        got = None
        # 1) rótulo canonico "Total de parcelas:"
        m = re.search(r"Total\s+de\s+parcelas[:\s]*(" + RE_MONEY.pattern + ")", text, re.I)
        if m:
            got = m.group(1)

        # 2) qualquer linha com "total" a partir do rodapé
        if not got:
            for ln in reversed(lines[-12:]): 
                if "total" in ln.lower():
                    m2 = RE_MONEY.search(ln)
                    if m2:
                        got = m2.group(0)
                        break

        # 3) fallback: último valor monetário do doc
        if not got:
            monies = RE_MONEY.findall(text)
            if monies:
                got = monies[-1]

        out["total_de_parcelas"] = got

    return out

# -------------------- Dispatcher por layout ----------------------------------
def extract_tela(keys: List[str], lines: List[str]) -> Dict[str, Optional[str]]:
    layout = detect_tela_layout(lines)
    if layout == "v2":
        return extract_tela_v2(keys, lines)
    if layout == "v3":
        return extract_tela_v3(keys, lines)
    return extract_tela_v1(keys, lines) 

# =============================================================================
#                                Dispatcher
# =============================================================================

def extract(label: str, schema: Dict[str, str], lines: List[str]) -> Dict[str, Optional[str]]:
    keys = list(schema.keys())
    if label == "carteira_oab":
        return extract_oab(keys, lines)
    if label == "tela_sistema":
        return extract_tela(keys, lines)
    return {k: None for k in keys}
