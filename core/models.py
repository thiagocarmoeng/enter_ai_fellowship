# core/models.py
from __future__ import annotations
from typing import Dict, Literal, Type
from pydantic import BaseModel, ConfigDict


# --------------------------------------------------------------------
# Base utilitário para gerar o dict de prompts (valores vazios)
# --------------------------------------------------------------------
class _BaseSchema(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=False,
        extra="ignore",
    )

    @classmethod
    def prompts(cls) -> Dict[str, str]:
        return {name: "" for name in cls.model_fields}

# --------------------------------------------------------------------
# OAB
# --------------------------------------------------------------------
class CarteiraOABSchema(_BaseSchema):
    nome: str = ""
    inscricao: str = ""
    seccional: str = ""
    subsecao: str = ""
    categoria: str = ""
    endereco_profissional: str = ""
    telefone_profissional: str = ""
    situacao: str = ""

# --------------------------------------------------------------------
# Telas de sistema (mantendo o typo 'data_verncimento')
# --------------------------------------------------------------------
class TelaSistemaV1Schema(_BaseSchema):
    # v1 (cadastro/consulta)
    data_base: str = ""
    data_verncimento: str = ""   
    quantidade_parcelas: str = ""
    produto: str = ""
    sistema: str = ""
    tipo_de_operacao: str = ""
    tipo_de_sistema: str = ""

class TelaSistemaV2Schema(_BaseSchema):
    # v2 (filtro/pesquisa)
    pesquisa_por: str = ""
    pesquisa_tipo: str = ""
    sistema: str = ""
    valor_parcela: str = ""
    cidade: str = ""

class TelaSistemaV3Schema(_BaseSchema):
    # v3 (detalhamento/total)
    data_referencia: str = ""
    selecao_de_parcelas: str = ""
    total_de_parcelas: str = ""

# Superset (fallback): união das três
class TelaSistemaSupersetSchema(TelaSistemaV1Schema, TelaSistemaV2Schema, TelaSistemaV3Schema):
    pass

TelaTipoLiteral = Literal["operacao", "consulta_cobranca", "detalhamento_saldos"]

# --------------------------------------------------------------------
# Helpers de fábrica – retornam os dicts de prompts (como seus SCHEMAS)
# --------------------------------------------------------------------
def schema_oab_prompts() -> Dict[str, str]:
    return CarteiraOABSchema.prompts()

def schema_tela_v1_prompts() -> Dict[str, str]:
    return TelaSistemaV1Schema.prompts()

def schema_tela_v2_prompts() -> Dict[str, str]:
    return TelaSistemaV2Schema.prompts()

def schema_tela_v3_prompts() -> Dict[str, str]:
    return TelaSistemaV3Schema.prompts()

def schema_tela_superset_prompts() -> Dict[str, str]:
    return TelaSistemaSupersetSchema.prompts()

# --------------------------------------------------------------------
# Roteamento por nome de arquivo (se precisar no CLI)
# --------------------------------------------------------------------
def infer_tela_tipo_from_filename(fn: str) -> TelaTipoLiteral | None:
    n = (fn or "").lower()
    if "tela_sistema_1" in n or n.endswith("_1.pdf"):
        return "operacao"
    if "tela_sistema_2" in n or n.endswith("_2.pdf"):
        return "consulta_cobranca"
    if "tela_sistema_3" in n or n.endswith("_3.pdf"):
        return "detalhamento_saldos"
    return None

def pick_tela_prompts_by_filename(fn: str) -> Dict[str, str]:
    t = infer_tela_tipo_from_filename(fn)
    if t == "operacao":
        return schema_tela_v1_prompts()
    if t == "consulta_cobranca":
        return schema_tela_v2_prompts()
    if t == "detalhamento_saldos":
        return schema_tela_v3_prompts()
    return schema_tela_superset_prompts()
