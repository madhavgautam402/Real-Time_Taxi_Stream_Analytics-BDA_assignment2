"""Stage 1: taxi.raw -> validate + enrich + Bloom test -> taxi.clean (Spark Structured Streaming)."""
import argparse, json, shutil, time
from datetime import datetime
from confluent_kafka import Producer
from pyspark.sql import functions as F
from common.bloom import make_bloom
from common.config import load_config, ROOT
from common.spark_utils import get_spark

TS_FMT = "%Y-%m-%dT%H:%M:%S"

def validate(ev):
    """Return (pickup_dt, dropoff_dt) if the event is valid, else None."""
    try:
        if not ev.get("event_id"):
            return None
        pz, dz = int(ev["pickup_zone"]), int(ev["dropoff_zone"])
        if not (1 <= pz <= 265 and 1 <= dz <= 265):
            return None
        pt = datetime.strptime(ev["event_time"], TS_FMT)
        dt = datetime.strptime(ev["dropoff_time"], TS_FMT)
        if dt <= pt or float(ev["fare_amount"]) < 0 or float(ev["trip_distance"]) < 0:
            return None
        return pt, dt
    except (KeyError, ValueError, TypeError):
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    ap.add_argument("--slots", type=int, default=4, help="Spark local[N] processing slots")
    ap.add_argument("--idle-timeout", type=int, default=0, help="stop after N s without data (0 = run forever)")
    ap.add_argument("--resume", action="store_true", help="keep checkpoint (restart experiment)")
    ap.add_argument("--key-mode", choices=["pickup_zone", "none"])
    a = ap.parse_args()

    cfg = load_config()
    run_id = a.run_id or cfg["run_id"]
    key_mode = a.key_mode or cfg["key_mode"]
    raw_t, clean_t = cfg["topics"]["raw"], cfg["topics"]["clean"]

    ck = ROOT / "checkpoints" / f"stage1_{run_id}"
    if not a.resume and ck.exists():
        shutil.rmtree(ck, ignore_errors=True)
    res = ROOT / "results"
    res.mkdir(exist_ok=True)

    bloom = make_bloom(cfg)
    exact_seen = set()                       # ground truth for Bloom evaluation
    st = dict(rows_in=0, accepted=0, rejected=0, pos=0, neg=0, fp=0, fn=0, fp_logged=0, last_data=None)

    producer = Producer({"bootstrap.servers": cfg["bootstrap"], "linger.ms": 5, "compression.type": "lz4"})

    def emit(key, value):
        while True:
            try:
                producer.produce(clean_t, key=key, value=value)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)

    def process(batch_df, batch_id):
        t_start = time.time()
        rows = batch_df.select("partition", "offset", F.col("value").cast("string").alias("v")).collect()
        if not rows:
            return
        parsed = []
        for r in rows:
            try:
                ev = json.loads(r["v"])
                parsed.append((ev.get("producer_ts_ms", 0), r["partition"], r["offset"], ev))
            except json.JSONDecodeError:
                st["rejected"] += 1
        parsed.sort(key=lambda x: x[:3])      # arrival order

        acc = 0
        for _, _, _, ev in parsed:
            st["rows_in"] += 1
            v = validate(ev)
            if v is None:
                st["rejected"] += 1
                continue
            pt, dt = v
            eid = ev["event_id"]

            truth_seen = eid in exact_seen
            bloom_seen = bloom.contains(eid)
            if truth_seen:
                st["pos"] += 1
                if not bloom_seen:
                    st["fn"] += 1
            else:
                st["neg"] += 1
                if bloom_seen:
                    st["fp"] += 1
                    if st["fp_logged"] < 20:
                        st["fp_logged"] += 1
                        with open(res / f"bloom_fp_examples_{run_id}.jsonl", "a") as f:
                            f.write(json.dumps({"event_id": eid, "ids_inserted_so_far": len(exact_seen),
                                                "note": "Bloom said seen, but ID was never seen before"}) + "\n")
            bloom.add(eid)
            exact_seen.add(eid)

            dur = (dt - pt).total_seconds() / 60.0
            out = dict(ev)
            out.update({
                "route_key": f"{ev['pickup_zone']}-{ev['dropoff_zone']}",
                "trip_duration_min": round(dur, 2),
                "slow_trip_bit": 1 if dur > 30 else 0,
                "bloom_seen_before": bool(bloom_seen),
                "seen_before_truth": bool(truth_seen),    # evaluation-only field
                "stage1_ts_ms": int(time.time() * 1000),
            })
            key = str(ev["pickup_zone"]) if key_mode == "pickup_zone" else None
            emit(key, json.dumps(out))
            acc += 1

        producer.flush()
        st["accepted"] += acc
        st["last_data"] = time.time()
        dur_s = max(time.time() - t_start, 1e-6)
        line = {"batch_id": batch_id, "rows": len(rows), "accepted": acc,
                "batch_s": round(dur_s, 3), "stage1_eps": round(acc / dur_s, 1),
                "ts_ms": int(time.time() * 1000)}
        with open(res / f"stage1_batches_{run_id}.jsonl", "a") as f:
            f.write(json.dumps(line) + "\n")
        print(f"[stage1] batch={batch_id} rows={len(rows)} {line['stage1_eps']}/s "
              f"total_accepted={st['accepted']} fp={st['fp']} fn={st['fn']}")

    def write_summary():
        s = {"run_id": run_id, "events_in": st["rows_in"], "accepted": st["accepted"],
             "rejected": st["rejected"], "ids_inserted": len(exact_seen),
             "bloom_bits": bloom.m, "bloom_hashes": bloom.k,
             "true_duplicates_seen": st["pos"], "false_positives": st["fp"], "false_negatives": st["fn"],
             "fp_rate": round(st["fp"] / st["neg"], 6) if st["neg"] else 0.0,
             "fn_rate": round(st["fn"] / st["pos"], 6) if st["pos"] else 0.0}
        with open(res / f"stage1_summary_{run_id}.json", "w") as f:
            json.dump(s, f, indent=2)
        print("[stage1] SUMMARY", json.dumps(s, indent=2))

    spark = get_spark(f"stage1-{run_id}", a.slots)
    spark.sparkContext.setLogLevel("WARN")
    src = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg["bootstrap"])
           .option("subscribe", raw_t)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", cfg.get("stage1_max_offsets_per_trigger", 20000))
           .load())
    q = (src.writeStream.foreachBatch(process)
         .option("checkpointLocation", ck.as_uri())
         .trigger(processingTime="1 second").start())
    print(f"[stage1] running: {raw_t} -> {clean_t}, slots={a.slots}, bloom m={bloom.m} k={bloom.k}")

    try:
        while not q.awaitTermination(1):
            if a.idle_timeout and st["last_data"] and time.time() - st["last_data"] > a.idle_timeout:
                print("[stage1] idle timeout reached, stopping")
                q.stop()
                break
    except KeyboardInterrupt:
        print("[stage1] interrupted")
        q.stop()
    finally:
        write_summary()
        spark.stop()

if __name__ == "__main__":
    main()