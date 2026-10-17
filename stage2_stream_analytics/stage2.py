"""Stage 2: taxi.clean -> streaming algorithms -> taxi.metrics (Spark Structured Streaming)."""
import argparse, itertools, json, random, shutil, sys, time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from types import SimpleNamespace
import mmh3
import numpy as np
from confluent_kafka import Consumer, Producer, TopicPartition
from pyspark.sql import functions as F
from common.config import load_config, ROOT
from common.sketches import DGIM, HyperLogLog, Reservoir
from common.spark_utils import get_spark

EPOCH = datetime(1970, 1, 1)

def iso(sec):
    return (EPOCH + timedelta(seconds=sec)).strftime("%Y-%m-%dT%H:%M:%S")

def err_pct(est, exact):
    return round(abs(est - exact) / exact * 100, 4) if exact else 0.0

def entity_selected(pz, frac):
    return mmh3.hash(str(pz), 17, signed=False) % 10000 < frac * 10000

def diverse_stats(zone_dests, thr):
    active = len(zone_dests)
    div = sum(1 for s in zone_dests.values() if len(s) >= thr)
    return (div / active if active else None), div, active

class Window:
    def __init__(self, start, end, cfg):
        self.start, self.end = start, end
        self.n = 0
        self.dest = defaultdict(set)          # exact: pickup zone -> set(dropoff zones)
        self.routes = set()                   # exact route set (ground truth)
        self.hll = HyperLogLog(cfg["hll_precision"])
        self.ent_dest = defaultdict(set)      # entity sampling state
        self.ent_events = 0
        self.ev_res = Reservoir(cfg["event_sample_cap"], seed=start)   # event sampling state
        self.frac = cfg["entity_sample_fraction"]

    def add(self, ev):
        self.n += 1
        pz, dz, rk = ev["pickup_zone"], ev["dropoff_zone"], ev["route_key"]
        self.dest[pz].add(dz)
        self.routes.add(rk)
        self.hll.add(rk)
        if entity_selected(pz, self.frac):
            self.ent_dest[pz].add(dz)
            self.ent_events += 1
        self.ev_res.add((pz, dz))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    ap.add_argument("--slots", type=int, default=4)
    ap.add_argument("--idle-timeout", type=int, default=0)
    ap.add_argument("--resume", action="store_true")
    a = ap.parse_args()

    cfg = load_config()
    run_id = a.run_id or cfg["run_id"]
    clean_t, metrics_t = cfg["topics"]["clean"], cfg["topics"]["metrics"]
    wsize = cfg["window_minutes"] * 60
    lateness = cfg["allowed_lateness_minutes"] * 60
    thr = cfg["diverse_min_destinations"]
    interval = cfg["report_interval_seconds"]
    res = ROOT / "results"
    res.mkdir(exist_ok=True)

    ck = ROOT / "checkpoints" / f"stage2_{run_id}"
    if not a.resume and ck.exists():
        shutil.rmtree(ck, ignore_errors=True)

    producer = Producer({"bootstrap.servers": cfg["bootstrap"], "linger.ms": 5})
    lagc = Consumer({"bootstrap.servers": cfg["bootstrap"], "group.id": f"lagprobe-{run_id}",
                     "enable.auto.commit": False})
    nparts = len(lagc.list_topics(clean_t, timeout=10).topics[clean_t].partitions)

    # ---------------- state ----------------
    st = SimpleNamespace(n=0, sum_fare=0.0, sum_dist=0.0, pos=0, neg=0, fp=0, fn=0,
                         late_inc=0, late_disc=0, late_logged=0, max_ev=0, last_data=None,
                         max_lag=0, max_off={}, part_counts=defaultdict(int), reports=0)
    iv = SimpleNamespace(n=0, busy=0.0, lat=[], t0=time.time())
    reservoirs = [Reservoir(k, seed=k) for k in cfg["reservoir_sizes"]]
    dgim = DGIM(cfg["dgim_max_window"], cfg.get("dgim_r", 2))
    bits = deque(maxlen=cfg["dgim_max_window"])
    windows = {}

    def send(obj):
        producer.produce(metrics_t, value=json.dumps(obj))

    def clean_lag():
        total = 0
        for p in range(nparts):
            lo, hi = lagc.get_watermark_offsets(TopicPartition(clean_t, p), timeout=5, cached=False)
            nxt = st.max_off.get(p, lo - 1) + 1
            total += max(0, hi - nxt)
        return total

    def log_late(ev, sec, ws, we, decision):
        if st.late_logged >= 30:
            return
        st.late_logged += 1
        with open(res / f"late_events_{run_id}.jsonl", "a") as f:
            f.write(json.dumps({
                "event_id": ev["event_id"], "event_time": ev["event_time"],
                "window_start": iso(ws), "window_end": iso(we),
                "watermark_event_time": iso(st.max_ev),
                "behind_watermark_min": round((st.max_ev - sec) / 60, 1),
                "decision": decision}) + "\n")

    # ---------------- reporting ----------------
    def build_report(final=False):
        now = time.time()
        wall = max(now - iv.t0, 1e-6)
        lat = np.array(iv.lat) if iv.lat else np.array([0])
        lag = clean_lag()
        st.max_lag = max(st.max_lag, lag)
        total_p = sum(st.part_counts.values()) or 1

        samp = {}
        for name, idx, tot in (("fare", 0, st.sum_fare), ("distance", 1, st.sum_dist)):
            exact = tot / st.n if st.n else 0
            samp[name] = {}
            for r in reservoirs:
                if r.items:
                    m = sum(x[idx] for x in r.items) / len(r.items)
                    samp[name][str(r.k)] = {"exact_mean": round(exact, 4), "sample_mean": round(m, 4),
                                            "sample_size": len(r.items), "error_pct": err_pct(m, exact)}
        arr = list(bits)
        dg = []
        for k in cfg["dgim_queries"]:
            kk = min(k, len(arr))
            ex = sum(arr[-kk:]) if kk else 0
            es = dgim.query(k)
            dg.append({"k": k, "effective_k": kk, "exact_ones": ex, "estimate": es,
                       "abs_error": abs(es - ex), "error_pct": err_pct(es, ex),
                       "buckets": dgim.num_buckets()})
        rep = {
            "type": "report", "run_id": run_id, "final": final, "report_ts_ms": int(now * 1000),
            "events_processed": st.n,
            "stage2_eps": round(iv.n / wall, 1),
            "stage2_capacity_eps": round(iv.n / iv.busy, 1) if iv.busy > 0 else 0,
            "consumer_lag": lag, "max_lag_so_far": st.max_lag,
            "latency_p50_ms": int(np.percentile(lat, 50)), "latency_p95_ms": int(np.percentile(lat, 95)),
            "partition_counts": dict(st.part_counts),
            "partition_skew": round(max(st.part_counts.values()) / total_p, 4) if st.part_counts else 0,
            "bloom": {"fp_rate": round(st.fp / st.neg, 6) if st.neg else 0.0,
                      "fn_rate": round(st.fn / st.pos, 6) if st.pos else 0.0,
                      "false_positives": st.fp, "false_negatives": st.fn,
                      "true_duplicates": st.pos, "true_new": st.neg},
            "sampling": samp, "dgim": dg,
            "late": {"included": st.late_inc, "discarded": st.late_disc},
        }
        send(rep)
        producer.poll(0)
        st.reports += 1
        d = next((x for x in dg if x["k"] == 5000), dg[-1])
        print(f"[stage2] REPORT n={st.n} eps={rep['stage2_eps']} lag={lag} "
              f"p50={rep['latency_p50_ms']}ms p95={rep['latency_p95_ms']}ms "
              f"bloom fp={st.fp} fn={st.fn} dgim(k={d['k']}) exact={d['exact_ones']} est={d['estimate']} "
              f"late inc/disc={st.late_inc}/{st.late_disc}")
        iv.n, iv.busy, iv.lat, iv.t0 = 0, 0.0, [], now
        return rep

    def finalize(w, final=False):
        exact_r, est = len(w.routes), w.hll.count()
        sample = list(itertools.islice(w.routes, 100))
        avg = sum(sys.getsizeof(s) for s in sample) / max(1, len(sample))
        exact_bytes = int(sys.getsizeof(w.routes) + avg * len(w.routes))

        ex_frac, ex_div, ex_act = diverse_stats(w.dest, thr)
        ent_frac, ent_div, ent_act = diverse_stats(w.ent_dest, thr)
        budget = w.ent_events                          # same storage budget for both methods
        pool = w.ev_res.items
        sub = random.Random(w.start).sample(pool, min(budget, len(pool))) if budget else []
        ev_dest = defaultdict(set)
        for pz, dz in sub:
            ev_dest[pz].add(dz)
        ev_frac, ev_div, ev_act = diverse_stats(ev_dest, thr)

        def ae(x):
            return round(abs(x - ex_frac), 4) if (x is not None and ex_frac is not None) else None

        msg = {
            "type": "window", "run_id": run_id, "final": final,
            "window_start": iso(w.start), "window_end": iso(w.end), "events_in_window": w.n,
            "emit_ts_ms": int(time.time() * 1000),
            "hll": {"exact_routes": exact_r, "estimate": est, "error_pct": err_pct(est, exact_r),
                    "precision": w.hll.p, "registers": w.hll.m,
                    "sketch_bytes": w.hll.memory_bytes(), "exact_set_bytes": exact_bytes},
            "diverse": {"threshold": thr, "active_zones": ex_act, "diverse_zones": ex_div,
                        "exact_fraction": None if ex_frac is None else round(ex_frac, 4),
                        "event_sampling": {"estimate": None if ev_frac is None else round(ev_frac, 4),
                                           "abs_error": ae(ev_frac), "events_stored": len(sub),
                                           "active_zones_seen": ev_act},
                        "entity_sampling": {"estimate": None if ent_frac is None else round(ent_frac, 4),
                                            "abs_error": ae(ent_frac), "events_stored": budget,
                                            "zones_selected": ent_act}},
        }
        send(msg)
        print(f"[stage2] WINDOW {msg['window_start']} n={w.n} | HLL exact={exact_r} est={est} "
              f"err={msg['hll']['error_pct']}% | diverse exact={msg['diverse']['exact_fraction']} "
              f"event={msg['diverse']['event_sampling']['estimate']} "
              f"entity={msg['diverse']['entity_sampling']['estimate']}")

    # ---------------- micro-batch ----------------
    def process(batch_df, batch_id):
        t_start = time.time()
        rows = batch_df.select("partition", "offset", F.col("value").cast("string").alias("v")).collect()
        if not rows:
            return
        parsed = []
        for r in rows:
            ev = json.loads(r["v"])
            parsed.append((ev.get("producer_ts_ms", 0), r["partition"], r["offset"], ev))
            st.part_counts[r["partition"]] += 1
            if r["offset"] > st.max_off.get(r["partition"], -1):
                st.max_off[r["partition"]] = r["offset"]
        parsed.sort(key=lambda x: x[:3])
        now_ms = int(time.time() * 1000)

        for pts, _, _, ev in parsed:
            st.n += 1
            iv.n += 1
            fare, dist = ev["fare_amount"], ev["trip_distance"]
            st.sum_fare += fare
            st.sum_dist += dist
            for r in reservoirs:
                r.add((fare, dist))
            b = ev["slow_trip_bit"]
            dgim.add(b)
            bits.append(b)
            iv.lat.append(now_ms - pts)

            truth, pred = ev["seen_before_truth"], ev["bloom_seen_before"]
            if truth:
                st.pos += 1
                st.fn += (not pred)
            else:
                st.neg += 1
                st.fp += bool(pred)

            sec = int((datetime.fromisoformat(ev["event_time"]) - EPOCH).total_seconds())
            ws = sec - sec % wsize
            we = ws + wsize
            if we + lateness <= st.max_ev:             # window already finalized
                st.late_disc += 1
                log_late(ev, sec, ws, we, "discarded (window already finalized)")
                continue
            if we <= st.max_ev:                        # late, but window still open (within lateness)
                st.late_inc += 1
                log_late(ev, sec, ws, we, "included (within allowed lateness)")
            w = windows.get(ws)
            if w is None:
                w = windows[ws] = Window(ws, we, cfg)
            w.add(ev)
            if sec > st.max_ev:
                st.max_ev = sec

        for ws in sorted(windows):                     # watermark-based finalization
            if windows[ws].end + lateness <= st.max_ev:
                finalize(windows.pop(ws))

        busy = time.time() - t_start
        iv.busy += busy
        st.last_data = time.time()
        if time.time() - iv.t0 >= interval:
            build_report()
        producer.poll(0)
        with open(res / f"stage2_batches_{run_id}.jsonl", "a") as f:
            f.write(json.dumps({"batch_id": batch_id, "rows": len(rows), "batch_s": round(busy, 3),
                                "ts_ms": int(time.time() * 1000)}) + "\n")

    spark = get_spark(f"stage2-{run_id}", a.slots)
    spark.sparkContext.setLogLevel("WARN")
    src = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg["bootstrap"])
           .option("subscribe", clean_t)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", cfg["stage2_max_offsets_per_trigger"])
           .load())
    q = (src.writeStream.foreachBatch(process)
         .option("checkpointLocation", ck.as_uri())
         .trigger(processingTime="1 second").start())
    print(f"[stage2] running: {clean_t} -> {metrics_t}, slots={a.slots}, partitions={nparts}")

    try:
        while not q.awaitTermination(1):
            if a.idle_timeout and st.last_data and time.time() - st.last_data > a.idle_timeout:
                print("[stage2] idle timeout reached, stopping")
                q.stop()
                break
    except KeyboardInterrupt:
        print("[stage2] interrupted")
        q.stop()
    finally:
        for ws in sorted(windows):                     # flush remaining windows at end of stream
            finalize(windows.pop(ws), final=True)
        if st.n:
            build_report(final=True)
        producer.flush()
        with open(res / f"stage2_summary_{run_id}.json", "w") as f:
            json.dump({"run_id": run_id, "events_processed": st.n, "reports": st.reports,
                       "max_lag": st.max_lag, "late_included": st.late_inc,
                       "late_discarded": st.late_disc}, f, indent=2)
        spark.stop()

if __name__ == "__main__":
    main()