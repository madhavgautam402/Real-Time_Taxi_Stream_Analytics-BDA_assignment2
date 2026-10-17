"""Final consumer: taxi.metrics + taxi.lsh -> console + results/results.jsonl (standard format)."""
import argparse, json, time
from confluent_kafka import Consumer
from common.config import load_config, ROOT

def append(path, obj):
    with open(path, "a", encoding="utf-8") as f:          # open/close per write: safe if killed
        f.write(json.dumps(obj) + "\n")

def producer_eps(res, run_id):
    p = res / f"producer_stats_{run_id}.json"
    return json.loads(p.read_text())["actual_eps"] if p.exists() else None

def build_record(rep, win, lsh, res, run_id):
    samp = None
    fare = rep["sampling"].get("fare", {})
    if fare:
        k = max(fare, key=int)
        x = fare[k]
        samp = {"k": int(k), "exact_mean_fare": x["exact_mean"],
                "sample_mean_fare": x["sample_mean"], "error_pct": x["error_pct"]}
    dg = next((d for d in rep["dgim"] if d["k"] == 5000), rep["dgim"][-1] if rep["dgim"] else None)
    lsh_default = None
    if lsh:
        c = lsh["configs"][0]
        lsh_default = {"candidates": c["candidates"], "true_pairs": lsh["true_pairs"],
                       "recall": c["recall"], "candidate_reduction": c["candidate_reduction"],
                       "bands": c["bands"], "rows": c["rows"]}
    now = int(time.time() * 1000)
    return {
        "run_id": run_id,
        "window_start": win["window_start"] if win else None,
        "window_end": win["window_end"] if win else None,
        "events_processed": rep["events_processed"],
        "producer_eps": producer_eps(res, run_id),
        "stage2_eps": rep["stage2_eps"],
        "consumer_lag": rep["consumer_lag"],
        "latency_p50_ms": rep["latency_p50_ms"],
        "latency_p95_ms": rep["latency_p95_ms"],
        "bloom": {"fp_rate": rep["bloom"]["fp_rate"], "fn_rate": rep["bloom"]["fn_rate"]},
        "sampling": samp,
        "hll": ({"exact_routes": win["hll"]["exact_routes"], "estimate": win["hll"]["estimate"],
                 "error_pct": win["hll"]["error_pct"]} if win else None),
        "dgim": ({"k": dg["k"], "exact_ones": dg["exact_ones"], "estimate": dg["estimate"],
                  "error_pct": dg["error_pct"]} if dg else None),
        "lsh": lsh_default,
        # ---- extra fields ----
        "final": rep["final"],
        "stage2_capacity_eps": rep.get("stage2_capacity_eps"),
        "max_lag": rep.get("max_lag_so_far"),
        "latency_overall_p50_ms": rep.get("latency_overall_p50_ms"),
        "latency_overall_p95_ms": rep.get("latency_overall_p95_ms"),
        "partition_skew": rep.get("partition_skew"),
        "bloom_detail": rep["bloom"], "sampling_all": rep["sampling"], "dgim_all": rep["dgim"],
        "late": rep.get("late"),
        "diverse": win["diverse"] if win else None,
        "lsh_all_configs": lsh["configs"] if lsh else None,
        "report_ts_ms": rep["report_ts_ms"], "final_ts_ms": now,
        "report_to_final_ms": now - rep["report_ts_ms"],
    }

def console_line(r):
    h = r["hll"]["error_pct"] if r["hll"] else "-"
    l = r["lsh"]["recall"] if r["lsh"] else "-"
    d = r["dgim"]["error_pct"] if r["dgim"] else "-"
    s = r["sampling"]["error_pct"] if r["sampling"] else "-"
    return (f"[final] {r['run_id']} n={r['events_processed']} eps={r['stage2_eps']} lag={r['consumer_lag']} "
            f"p50={r['latency_p50_ms']}ms p95={r['latency_p95_ms']}ms bloom_fp={r['bloom']['fp_rate']} "
            f"samp_err={s}% hll_err={h}% dgim_err={d}% lsh_recall={l}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    ap.add_argument("--idle-timeout", type=int, default=0)
    a = ap.parse_args()
    cfg = load_config()
    run_id = a.run_id or cfg["run_id"]
    res = ROOT / "results"
    res.mkdir(exist_ok=True)

    c = Consumer({"bootstrap.servers": cfg["bootstrap"], "group.id": f"final-{run_id}-{int(time.time())}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([cfg["topics"]["metrics"], cfg["topics"]["lsh"]])
    print(f"[final] consuming {cfg['topics']['metrics']} + {cfg['topics']['lsh']} for run_id={run_id}")

    last_win = last_lsh = None
    last_msg = time.time()
    try:
        while True:
            m = c.poll(1.0)
            if m is None:
                if a.idle_timeout and time.time() - last_msg > a.idle_timeout:
                    break
                continue
            if m.error():
                continue
            last_msg = time.time()
            d = json.loads(m.value())
            if d.get("run_id") != run_id:
                continue
            t = d["type"]
            if t in ("window", "lsh"):
                append(res / f"windows_{run_id}.jsonl", d)
                if t == "window":
                    last_win = d
                else:
                    last_lsh = d
            elif t == "report":
                rec = build_record(d, last_win, last_lsh, res, run_id)
                append(res / "results.jsonl", rec)
                append(res / f"results_{run_id}.jsonl", rec)
                print(console_line(rec), flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    main()