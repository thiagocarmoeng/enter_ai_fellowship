#!/usr/bin/env python3
from __future__ import annotations
import json, os, time, csv
from pathlib import Path
from typing import Dict, List, Tuple, Any
import argparse
import sys

from core.orchestrator import run_extract
from core.llm_client import get_cost_summary, reset_usage

# ---------- parâmetros (ENV) --------------------------------------------------
SLA_LATENCY_S   = float(os.getenv("SLA_LATENCY_S", "10"))    # média < 10s
ACC_MIN_DOC     = float(os.getenv("ACC_MIN_DOC", "0.80"))    # doc ok se ≥ 80%
ACC_MIN_GLOBAL  = float(os.getenv("ACC_MIN_GLOBAL", "0.80")) # média global ≥ 80%
FAIL_ON_SLA     = os.getenv("FAIL_ON_SLA", "0") == "1"       # exit 1 se violar
SHOW_TOP_N      = int(os.getenv("SHOW_TOP_N", "3"))

# ---------- utils -------------------------------------------------------------
def _resolve(p: str) -> Path:
    return Path(p).expanduser().resolve()

def norm(s: Any) -> str:
    return (str(s) if s is not None else "").strip().casefold()

def compare_fields(golden: Dict[str, Any], got: Dict[str, Any]) -> Tuple[int,int,Dict[str,bool]]:
    total = 0
    correct = 0
    per_field: Dict[str, bool] = {}
    # compara SOMENTE as chaves do golden
    for k, gold_v in golden.items():
        total += 1
        ok = norm(gold_v) == norm(got.get(k, ""))
        per_field[k] = ok
        if ok:
            correct += 1
    return correct, total, per_field

def load_tasks_from_json(dataset_path: str) -> List[Dict[str, Any]]:
    p = _resolve(dataset_path)
    if not p.exists():
        cwd = Path.cwd()
        raise FileNotFoundError(f"dataset não encontrado: {p}\nCWD: {cwd}")
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("dataset.json deve conter uma lista de tarefas")
    return data

def percentile(values: List[float], p: float) -> float:
    """Percentil simples (robusto para N pequeno)."""
    if not values:
        return 0.0
    if p <= 0: return min(values)
    if p >= 100: return max(values)
    xs = sorted(values)
    k = (len(xs)-1) * (p/100.0)
    f = int(k)
    c = min(f+1, len(xs)-1)
    if f == c:
        return xs[f]
    d0 = xs[f] * (c - k)
    d1 = xs[c] * (k - f)
    return d0 + d1

# ---------- benchmark ---------------------------------------------------------
def run_benchmark(dataset_path: str, pdf_dir: str, use_llm: bool = False, verbose: bool = False, csv_out: str | None = None) -> int:
    tasks = load_tasks_from_json(dataset_path)
    pdf_root = _resolve(pdf_dir)

    per_doc: List[Tuple[str, str, float, float]] = []  # (label, pdf, latency, acc)
    per_label_stats: Dict[str, Dict[str, Any]] = {}    # label -> {"lat":[], "right":0,"total":0, "docs":[]}

    # opcional: armazenar misses por doc quando verbose
    misses_by_doc: Dict[str, List[str]] = {}

    reset_usage()

    for t in tasks:
        label = t.get("label")
        pdf_rel = t.get("pdf_path")
        golden = t.get("extraction_schema")

        if not label or not pdf_rel or not isinstance(golden, dict) or not golden:
            continue

        pdf_path = _resolve(pdf_rel) if os.path.isabs(pdf_rel) else (pdf_root / pdf_rel)
        if not pdf_path.exists():
            print(f"[SKIP] PDF não encontrado: {pdf_path}")
            continue

        # --- extração protegida ---
        tic = time.perf_counter()
        got: Dict[str, Any] = {}
        try:
            got = run_extract(label, golden, str(pdf_path), use_llm=use_llm)
        except Exception as e:
            print(f"[ERRO] Falha ao extrair '{pdf_path.name}': {e}")
            # import traceback; traceback.print_exc()
            got = {}
        toc = time.perf_counter()

        latency = toc - tic
        right, total, per = compare_fields(golden, got)
        acc = (right / total) if total else 0.0

        if verbose:
            miss = [k for k, ok in per.items() if not ok]
            if miss:
                print(f"[MISS] {pdf_path.name}: {', '.join(miss)}")
                misses_by_doc[pdf_path.name] = miss

        per_doc.append((label, pdf_path.name, latency, acc))

        st = per_label_stats.setdefault(label, {"lat": [], "right": 0, "total": 0, "docs": []})
        st["lat"].append(latency)
        st["right"] += right
        st["total"] += total
        st["docs"].append((pdf_path.name, latency, acc))

    # ----- agregados gerais -----
    latencies = [x[2] for x in per_doc]
    accs      = [x[3] for x in per_doc]
    n_docs    = len(per_doc)

    cost = get_cost_summary()
    usd_total = cost.get("usd_total", 0.0)
    usd_per_doc = (usd_total / n_docs) if n_docs else 0.0

    def pct(x: float) -> str:
        return f"{round(100*x, 2)}%"

    print("\n=== RESULTADOS GERAIS ===")
    if latencies:
        print(f"Latência média: {sum(latencies)/len(latencies):.3f}s")
        print(f"p95: {percentile(latencies,95):.3f}s | p99: {percentile(latencies,99):.3f}s")
    if accs:
        avg_acc = sum(accs)/len(accs)
        ok_docs = sum(1 for a in accs if a >= ACC_MIN_DOC)
        print(f"Precisão média (campos): {pct(avg_acc)}")
        print(f"Docs ≥{int(ACC_MIN_DOC*100)}%: {ok_docs}/{len(accs)}")

    print(f"Custo total (USD): {usd_total:.6f} | Custo médio/doc (USD): {usd_per_doc:.6f}")
    print(f"Tokens: prompt={cost.get('prompt_tokens',0)} | completion={cost.get('completion_tokens',0)}")

    print("\n=== POR LABEL ===")
    for label, st in per_label_stats.items():
        avg_lat = (sum(st["lat"])/len(st["lat"])) if st["lat"] else 0.0
        acc = (st["right"]/st["total"]) if st["total"] else 1.0
        print(f"- {label}: lat_média={avg_lat:.3f}s | precisão={pct(acc)} ({st['right']}/{st['total']})")

        if st["docs"]:
            worst_lat = sorted(st["docs"], key=lambda r: r[1], reverse=True)[:SHOW_TOP_N]
            worst_acc = sorted(st["docs"], key=lambda r: r[2])[:SHOW_TOP_N]
            print(f"  · Mais lentos: {', '.join(f'{n}={l:.2f}s' for n,l,_ in worst_lat)}")
            print(f"  · Menor precisão: {', '.join(f'{n}={pct(a)}' for n,_,a in worst_acc)}")

    # ----- CSV opcional -----
    if csv_out and per_doc:
        outp = _resolve(csv_out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        with outp.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["label", "pdf", "latency_s", "accuracy_frac", "misses"])
            for (label, pdf, lat, acc) in per_doc:
                miss_list = misses_by_doc.get(pdf, [])
                w.writerow([label, pdf, f"{lat:.6f}", f"{acc:.6f}", ";".join(miss_list)])
        print(f"\n[OK] CSV salvo em: {outp}")

    # ----- checagem de SLA/meta -----
    exit_code = 0
    mean_lat_ok = True if not latencies else (sum(latencies)/len(latencies) <= SLA_LATENCY_S)
    mean_acc_ok = True if not accs else (sum(accs)/len(accs) >= ACC_MIN_GLOBAL)

    print("\n=== SLA / METAS ===")
    print(f"- Latência média <= {SLA_LATENCY_S:.1f}s: {'OK' if mean_lat_ok else 'FAIL'}")
    print(f"- Precisão média >= {int(ACC_MIN_GLOBAL*100)}%: {'OK' if mean_acc_ok else 'FAIL'}")

    if FAIL_ON_SLA and (not mean_lat_ok or not mean_acc_ok):
        exit_code = 1

    if n_docs == 0:
        print("\n[WARN] Nenhum documento processado (verifique caminhos e dataset).")
    return exit_code

# ---------- CLI ---------------------------------------------------------------
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("dataset")
    p.add_argument("pdf_dir")
    p.add_argument("--llm", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--csv", help="Caminho do CSV de saída (opcional)")
    args = p.parse_args()

    rc = run_benchmark(args.dataset, args.pdf_dir, use_llm=args.llm, verbose=args.verbose, csv_out=args.csv)
    sys.exit(rc)
