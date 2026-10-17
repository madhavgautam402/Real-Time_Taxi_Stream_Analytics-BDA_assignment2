import argparse, csv, json
from common.config import ROOT

FIELDS = ["run_id", "partitions", "slots", "key_mode", "requested_rate", "produced_eps",
          "stage1_eps", "stage2_eps", "latency_p50_ms", "latency_p95_ms", "max_lag", "max_raw_lag", "partition_skew",
          "bloom_fp_rate", "sample_err_pct", "hll_err_pct_mean", "dgim_err_pct", "lsh_recall_mean",
          "late_included", "late_discarded", "events_processed"]

def jl(rel):
    p = ROOT / rel
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()] if p.exists() else []

def eps(rows):
    if len(rows) < 2:
        return 0.0
    span = (rows[-1]["ts_ms"] - rows[0]["ts_ms"]) / 1000
    n = sum(r["rows"] for r in rows[1:])
    return round(n / span, 1) if span > 0 else 0.0

def mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 4) if xs else None

def summarize(run_id, **extra):
    res = "results"
    pp = ROOT / res / f"producer_stats_{run_id}.json"
    prod = json.loads(pp.read_text()) if pp.exists() else {}
    recs = jl(f"{res}/results_{run_id}.jsonl")
    wins = jl(f"{res}/windows_{run_id}.jsonl")
    final = next((r for r in reversed(recs) if r.get("final")), recs[-1] if recs else {})
    row = {
        "run_id": run_id, **extra,
        "produced_eps": prod.get("actual_eps"),
        "stage1_eps": eps(jl(f"{res}/stage1_batches_{run_id}.jsonl")),
        "stage2_eps": eps(jl(f"{res}/stage2_batches_{run_id}.jsonl")),
        "latency_p50_ms": final.get("latency_overall_p50_ms"),
        "latency_p95_ms": final.get("latency_overall_p95_ms"),
        "max_lag": final.get("max_lag"),
        "max_raw_lag": max([r.get("raw_lag", 0) for r in jl(f"{res}/stage1_batches_{run_id}.jsonl")][2:] or [0]),        "partition_skew": prod.get("partition_skew_max_fraction"),
        "bloom_fp_rate": (final.get("bloom") or {}).get("fp_rate"),
        "sample_err_pct": (final.get("sampling") or {}).get("error_pct"),
        "hll_err_pct_mean": mean([w["hll"]["error_pct"] for w in wins if w.get("type") == "window"]),
        "dgim_err_pct": (final.get("dgim") or {}).get("error_pct"),
        "lsh_recall_mean": mean([w["configs"][0]["recall"] for w in wins if w.get("type") == "lsh"]),
        "late_included": (final.get("late") or {}).get("included"),
        "late_discarded": (final.get("late") or {}).get("discarded"),
        "events_processed": final.get("events_processed"),
    }
    out = ROOT / res / "experiment_summary.csv"
    rows = []
    if out.exists():
        with open(out, newline="", encoding="utf-8") as f:
            rows = [r for r in csv.DictReader(f) if r["run_id"] != run_id]
    rows.append({k: row.get(k) for k in FIELDS})
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    return row

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    a = ap.parse_args()
    print(json.dumps(summarize(a.run_id), indent=2))