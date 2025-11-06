#!/usr/bin/env python3
# app/cli.py
from __future__ import annotations

import json
import os
import sys
import pathlib
from typing import Dict, List, Any

from dotenv import load_dotenv
load_dotenv()  # carrega .env da raiz

from core.orchestrator import run_extract
from core.schemas import ExtractRequest

from concurrent.futures import ThreadPoolExecutor, as_completed

from core.models import (
    schema_oab_prompts,
    # pick_tela_prompts_by_filename,
    infer_tela_tipo_from_filename,
)

def parse_jobs(argv: List[str]) -> int:
    for a in argv:
        if a.startswith("--jobs="):
            try:
                return max(1, int(a.split("=",1)[1]))
            except Exception:
                pass
    return int(os.getenv("CLI_JOBS", "1"))

def process_task(task, pdf_dir, mode, use_llm_flag):
    from core.schemas import ExtractRequest
    label = None
    try:
        req = ExtractRequest(**{k: task[k] for k in ("label","extraction_schema","pdf_path")})
        label = req.label
        prompts_schema = req.extraction_schema
        pdf_rel = req.pdf_path
        tela_tipo = task.get("tela_sistema_tipo") if label == "tela_sistema" else None
        pdf_path = pdf_rel if os.path.isabs(pdf_rel) else os.path.join(pdf_dir, pdf_rel)
        if not os.path.exists(pdf_path):
            return ("skip", task, f"PDF não encontrado: {pdf_path}")

        values = None
        if mode in {"filled","both"}:
            kwargs = {"use_llm": use_llm_flag}
            if tela_tipo:
                kwargs["tela_sistema_tipo"] = tela_tipo
            values = run_extract(label, prompts_schema, pdf_path, **kwargs)
            values = {k: ("" if v is None else v) for k, v in values.items()}

        item = {
            "label": label,
            "pdf_path": pdf_rel,
        }
        if label == "tela_sistema" and tela_tipo:
            item["tela_sistema_tipo"] = tela_tipo

        if mode == "filled":
            item["extraction_schema"] = values
        elif mode == "prompts":
            item["extraction_schema"] = prompts_schema
        else:  # both
            item["extraction_schema"] = prompts_schema
            item["extracted"] = values
        return ("ok", item, None)

    except Exception as e:
        return ("err", {"label": label, "task": task}, str(e))

# ---------------- utils ----------------
def usage_and_exit() -> None:
    print(
        "Uso:\n"
        "  (dataset)   python -m app.cli /caminho/dataset.json /caminho/pdfs [outputs_dir|consolidado.json] [--mode=filled|prompts|both] [--llm]\n"
        "  (diretório) python -m app.cli /caminho/pdfs [outputs_dir|consolidado.json] [--mode=filled|prompts|both] [--llm]\n"
        "Obs.: Se o terceiro argumento terminar em .json, gera saída consolidada única.\n"
        "      --mode=prompts mantém 'extraction_schema' (descrições); --mode=both adiciona 'extracted' com valores.\n"
        "      --llm ativa fallback LLM para completar campos faltantes.",
        file=sys.stderr
    )
    sys.exit(1)

def parse_mode(argv: List[str]) -> str:
    mode = "filled"
    for a in argv:
        if a.startswith("--mode="):
            mode = a.split("=", 1)[1].strip().lower()
    if mode not in {"filled", "prompts", "both"}:
        print(f"[WARN] --mode inválido '{mode}', usando 'filled'", file=sys.stderr)
        mode = "filled"
    return mode

def parse_llm(argv: List[str]) -> bool:
    return ("--llm" in argv) or (os.getenv("EXTRACT_USE_LLM") == "1")

def write_json_atomic(path: str, data: Any) -> None:
    out_dir = os.path.dirname(path) or "."
    os.makedirs(out_dir, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)

# ---------------- carregamento de tarefas ----------------
def load_tasks_from_dataset(dataset_path: str) -> List[Dict[str, Any]]:
    with open(dataset_path, "r", encoding="utf-8") as f:
        tasks = json.load(f)
    if not isinstance(tasks, list):
        raise ValueError("dataset.json deve conter uma lista de tarefas")

    for t in tasks:
        try:
            if t.get("label") == "tela_sistema" and "tela_sistema_tipo" not in t:
                tt = infer_tela_tipo_from_filename(str(t.get("pdf_path", "")))
                if tt:
                    t["tela_sistema_tipo"] = tt
        except Exception:
            pass
    return tasks

def load_tasks_from_dir(pdf_dir: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for fn in sorted(os.listdir(pdf_dir)):
        if fn.startswith(".") or not fn.lower().endswith(".pdf"):
            continue
        lower = fn.lower()
        if "oab" in lower:
            schema = schema_oab_prompts()
            items.append({
                "label": "carteira_oab",
                "extraction_schema": schema,
                "pdf_path": fn
            })
        else:
            # INDEPENDENTE DO NOME DO ARQUIVO - sempre superset
            from core.models import schema_tela_superset_prompts
            schema = schema_tela_superset_prompts()
            items.append({
                "label": "tela_sistema",
                "extraction_schema": schema,
                "pdf_path": fn
            })
    if not items:
        print(f"[WARN] Nenhum PDF encontrado em: {pdf_dir}", file=sys.stderr)
    return items


# ---------------- main ----------------
def main():
    if len(sys.argv) < 2:
        usage_and_exit()

    mode = parse_mode(sys.argv[1:])
    use_llm_flag = parse_llm(sys.argv[1:])
    need_extract = mode in {"filled", "both"}

    # ---- helpers locais: layout scoring/poda ----
    V1_KEYS = ("data_base", "data_verncimento", "quantidade_parcelas",
               "produto", "sistema", "tipo_de_operacao", "tipo_de_sistema")
    V2_KEYS = ("pesquisa_por", "pesquisa_tipo", "sistema", "valor_parcela", "cidade")
    V3_KEYS = ("data_referencia", "selecao_de_parcelas", "total_de_parcelas")

    def _score(values: dict, keys: tuple[str, ...]) -> int:
        return sum(1 for k in keys if values.get(k))

    def best_tela_layout(values: dict) -> str:
        s1 = _score(values, V1_KEYS)
        s2 = _score(values, V2_KEYS)
        s3 = _score(values, V3_KEYS)
        ranked = sorted([("v1", s1), ("v2", s2), ("v3", s3)],
                        key=lambda t: (t[1], {"v1":1,"v2":2,"v3":3}[t[0]]),
                        reverse=True)
        return ranked[0][0]

    def subset_for_layout(values: dict, layout: str) -> dict:
        if layout == "v1":
            order = V1_KEYS
        elif layout == "v2":
            order = V2_KEYS
        else:
            order = V3_KEYS
        return {k: (values.get(k) or "") for k in order}

    # ---- args sem flags ----
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        usage_and_exit()
    arg1 = args[0]

    # ----- MODO DATASET -----
    if arg1.lower().endswith(".json") and os.path.isfile(arg1):
        if len(args) < 2:
            usage_and_exit()
        dataset_path = arg1
        pdf_dir = args[1]
        out_arg = args[2] if len(args) > 2 else os.path.join(os.getcwd(), "outputs")
        tasks = load_tasks_from_dataset(dataset_path)

    # ----- MODO DIRETÓRIO -----
    elif os.path.isdir(arg1):
        pdf_dir = arg1
        out_arg = args[1] if len(args) > 1 else os.path.join(os.getcwd(), "outputs")
        tasks = load_tasks_from_dir(pdf_dir)

    else:
        usage_and_exit()
        return

    consolidate = out_arg.lower().endswith(".json")
    if consolidate:
        out_consolidated_path = out_arg
        results: List[Dict[str, Any]] = []
    else:
        out_dir = out_arg
        os.makedirs(out_dir, exist_ok=True)

    processed = 0
    for task in tasks:
        try:
            req = ExtractRequest(**{k: task[k] for k in ("label", "extraction_schema", "pdf_path")})
        except Exception:
            print(f"[SKIP] Task inválida: {task}", file=sys.stderr)
            continue

        label = req.label
        prompts_schema = req.extraction_schema
        pdf_rel = req.pdf_path

        pdf_path = pdf_rel if os.path.isabs(pdf_rel) else os.path.join(pdf_dir, pdf_rel)
        if not os.path.exists(pdf_path):
            print(f"[SKIP] PDF não encontrado: {pdf_path}", file=sys.stderr)
            continue

        # Executa a extração
        values = None
        if need_extract:
            try:
                values = run_extract(label, prompts_schema, pdf_path, use_llm=use_llm_flag)
                values = {k: ("" if v is None else v) for k, v in values.items()}
                # >>> poda de layout só para tela_sistema no modo filled
                if label == "tela_sistema" and mode == "filled":
                    layout = best_tela_layout(values)
                    values = subset_for_layout(values, layout)
            except Exception as e:
                print(f"[ERRO] Falha ao extrair '{pdf_rel}': {e}", file=sys.stderr)
                continue

        # ----- saída -----
        if consolidate:
            if mode == "filled":
                item = {"label": label, "extraction_schema": values, "pdf_path": pdf_rel}
            elif mode == "prompts":
                item = {"label": label, "extraction_schema": prompts_schema, "pdf_path": pdf_rel}
            else:  # both
                item = {
                    "label": label,
                    "extraction_schema": prompts_schema,
                    "extracted": values,
                    "pdf_path": pdf_rel
                }
            results.append(item)
        else:
            base = pathlib.Path(pdf_rel).stem + ".json"
            out_path = os.path.join(out_dir, base)
            try:
                if mode == "prompts":
                    write_json_atomic(out_path, prompts_schema)
                else:
                    if values is None:
                        print(f"[SKIP] '{pdf_rel}': nada a escrever (sem extração). Use --mode=prompts para salvar os prompts.", file=sys.stderr)
                        continue
                    # mesma poda no modo por-arquivo
                    if label == "tela_sistema" and mode == "filled":
                        layout = best_tela_layout(values)
                        values = subset_for_layout(values, layout)
                    write_json_atomic(out_path, values)
                print(f"[OK] {pdf_rel} -> {out_path}")
            except Exception as e:
                print(f"[ERRO] Falha ao escrever '{out_path}': {e}", file=sys.stderr)
                continue

        processed += 1
    if consolidate:
        try:
            write_json_atomic(out_consolidated_path, results)
            print(f"[OK] Consolidado {processed} arquivos -> {out_consolidated_path}")
        except Exception as e:
            print(f"[ERRO] Falha ao escrever consolidado: {e}", file=sys.stderr)
            sys.exit(3)
    else:
        print(f"[OK] Processados: {processed} | Saída: {os.path.abspath(out_dir)}")

if __name__ == "__main__":
    main()
