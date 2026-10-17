"""Stage 3: taxi.clean -> per-window destination sets -> MinHash-LSH -> taxi.lsh."""
import argparse, json, shutil, time
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import combinations
from types import SimpleNamespace
from confluent_kafka import Producer
from pyspark.sql import functions as F
from common.config import load_config, ROOT
from common.minhash import MinHasher, jaccard, lsh_candidates
from common.spark_utils import get_spark

EPOCH = datetime(1970, 1, 1)

def iso(sec):
    return (EPOCH + timedelta(seconds=sec)).strftime("%Y-%m-%dT%H:%M:%S")

class Win:
    def __init__(self, start, end):
        self.start, self.end, self.n = start, end, 0
        self.cnt = defaultdict(int)          # trips per pickup zone
        self.dest = defaultdict(set)         # S(P): dropoff zones per pickup zone

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    ap.add_argument("--slots", type=int, default=4)
    ap.add_argument("--idle-timeout", type=int, default=0)
    ap.add_argument("--resume", action="store_true")
    a = ap.parse_args()

    cfg = load_config()
    run_id = a.run_id or cfg["run_id"]
    clean_t, lsh_t = cfg["topics"]["clean"], cfg["topics"]["lsh"]
    wsize = cfg["window_minutes"] * 60
    lateness = cfg["allowed_lateness_minutes"] * 60
    min_trips, thr, nh = cfg["lsh_min_trips"], cfg["jaccard_threshold"], cfg["lsh_num_hashes"]
    configs = [(cfg["lsh_bands"], cfg["lsh_rows_per_band"])] + [tuple(c) for c in cfg.get("lsh_extra_configs", [])]
    for b, r in configs:
        assert b * r == nh, f"bands*rows must equal lsh_num_hashes ({b}x{r} != {nh})"
    hasher = MinHasher(nh)

    res = ROOT / "results"
    res.mkdir(exist_ok=True)
    ck = ROOT / "checkpoints" / f"stage3_{run_id}"
    if not a.resume and ck.exists():
        shutil.rmtree(ck, ignore_errors=True)

    producer = Producer({"bootstrap.servers": cfg["bootstrap"], "linger.ms": 5})
    st = SimpleNamespace(n=0, late_inc=0, late_disc=0, max_ev=0, last_data=None, windows=0)
    windows = {}

    def finalize(w, final=False):
        zones = sorted(z for z, n in w.cnt.items() if n >= min_trips)
        sets = {z: w.dest[z] for z in zones}
        total_pairs = len(zones) * (len(zones) - 1) // 2

        # ---- exact all-pairs baseline (evaluation ground truth) ----
        t0 = time.perf_counter()
        true_pairs = {}
        for z1, z2 in combinations(zones, 2):
            j = jaccard(sets[z1], sets[z2])
            if j >= thr:
                true_pairs[(z1, z2)] = j
        exact_ms = (time.perf_counter() - t0) * 1000

        # ---- MinHash signatures (shared by all band/row settings) ----
        t0 = time.perf_counter()
        sigs = {z: hasher.signature(sets[z]) for z in zones}
        sig_ms = (time.perf_counter() - t0) * 1000

        cfg_out, sample_pairs = [], []
        for idx, (b, r) in enumerate(configs):
            t1 = time.perf_counter()
            cands = lsh_candidates(sigs, b, r)
            cand_j, returned = {}, {}
            for p in cands:                                  # exact Jaccard only on candidates
                j = jaccard(sets[p[0]], sets[p[1]])
                cand_j[p] = j
                if j >= thr:
                    returned[p] = j
            lsh_ms = sig_ms + (time.perf_counter() - t1) * 1000
            n_c = len(cands)
            cfg_out.append({
                "bands": b, "rows": r, "candidates": n_c, "candidate_true": len(returned),
                "returned_pairs": len(returned),
                "recall": round(len(returned) / len(true_pairs), 4) if true_pairs else None,
                "candidate_precision": round(len(returned) / n_c, 4) if n_c else None,
                "candidate_reduction": round(1 - n_c / total_pairs, 4) if total_pairs else None,
                "lsh_latency_ms": round(lsh_ms, 2)})
            if idx == 0:                                     # default setting -> demo output
                top = sorted(cand_j.items(), key=lambda kv: -kv[1])
                sample_pairs = [{"zones": list(p), "jaccard": round(j, 4), "is_true_pair": j >= thr}
                                for p, j in top[:10]]
                with open(res / f"lsh_pairs_{run_id}.jsonl", "a") as f:
                    f.write(json.dumps({"window_start": iso(w.start), "bands": b, "rows": r,
                                        "candidates": [[p[0], p[1], round(j, 4)] for p, j in top]}) + "\n")

        msg = {"type": "lsh", "run_id": run_id, "final": final,
               "window_start": iso(w.start), "window_end": iso(w.end), "events_in_window": w.n,
               "min_trips": min_trips, "jaccard_threshold": thr,
               "active_zones": len(zones), "total_pairs": total_pairs, "true_pairs": len(true_pairs),
               "exact_latency_ms": round(exact_ms, 2), "signature_ms": round(sig_ms, 2),
               "configs": cfg_out, "sample_pairs": sample_pairs,
               "emit_ts_ms": int(time.time() * 1000)}
        producer.produce(lsh_t, value=json.dumps(msg))
        producer.poll(0)
        st.windows += 1
        parts = " | ".join(f"{c['bands']}x{c['rows']} cand={c['candidates']} recall={c['recall']} "
                           f"red={c['candidate_reduction']} {c['lsh_latency_ms']}ms" for c in cfg_out)
        print(f"[stage3] WINDOW {msg['window_start']} zones={len(zones)} true_pairs={len(true_pairs)} "
              f"exact={exact_ms:.1f}ms | {parts}")

    def process(batch_df, batch_id):
        t_start = time.time()
        rows = batch_df.select("partition", "offset", F.col("value").cast("string").alias("v")).collect()
        if not rows:
            return
        parsed = []
        for r in rows:
            ev = json.loads(r["v"])
            parsed.append((ev.get("producer_ts_ms", 0), r["partition"], r["offset"], ev))
        parsed.sort(key=lambda x: x[:3])

        for _, _, _, ev in parsed:
            st.n += 1
            sec = int((datetime.fromisoformat(ev["event_time"]) - EPOCH).total_seconds())
            ws = sec - sec % wsize
            we = ws + wsize
            if we + lateness <= st.max_ev:               # window already finalized -> discard
                st.late_disc += 1
                continue
            if we <= st.max_ev:                          # late but window still open -> include
                st.late_inc += 1
            w = windows.get(ws)
            if w is None:
                w = windows[ws] = Win(ws, we)
            w.n += 1
            w.cnt[ev["pickup_zone"]] += 1
            w.dest[ev["pickup_zone"]].add(ev["dropoff_zone"])
            if sec > st.max_ev:
                st.max_ev = sec

        for ws in sorted(windows):
            if windows[ws].end + lateness <= st.max_ev:
                finalize(windows.pop(ws))

        st.last_data = time.time()
        with open(res / f"stage3_batches_{run_id}.jsonl", "a") as f:
            f.write(json.dumps({"batch_id": batch_id, "rows": len(rows),
                                "batch_s": round(time.time() - t_start, 3),
                                "ts_ms": int(time.time() * 1000)}) + "\n")

    spark = get_spark(f"stage3-{run_id}", a.slots)
    spark.sparkContext.setLogLevel("WARN")
    src = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg["bootstrap"])
           .option("subscribe", clean_t)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", cfg["stage3_max_offsets_per_trigger"])
           .load())
    q = (src.writeStream.foreachBatch(process)
         .option("checkpointLocation", ck.as_uri())
         .trigger(processingTime="1 second").start())
    print(f"[stage3] running: {clean_t} -> {lsh_t}, slots={a.slots}, configs={configs}")

    try:
        while not q.awaitTermination(1):
            if a.idle_timeout and st.last_data and time.time() - st.last_data > a.idle_timeout:
                print("[stage3] idle timeout reached, stopping")
                q.stop()
                break
    except KeyboardInterrupt:
        print("[stage3] interrupted")
        q.stop()
    finally:
        for ws in sorted(windows):
            finalize(windows.pop(ws), final=True)
        producer.flush()
        with open(res / f"stage3_summary_{run_id}.json", "w") as f:
            json.dump({"run_id": run_id, "events_processed": st.n, "windows": st.windows,
                       "late_included": st.late_inc, "late_discarded": st.late_disc}, f, indent=2)
        spark.stop()

if __name__ == "__main__":
    main()