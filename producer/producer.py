"""Replays taxi trips into taxi.raw with duplicates + delayed events."""
import argparse, heapq, json, random, time
import pandas as pd
from confluent_kafka import Producer
from common.config import load_config, resolve, ROOT

COLS = {
    "pickup":  ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"],
    "dropoff": ["tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime"],
    "pu":      ["PULocationID"],
    "do":      ["DOLocationID"],
    "dist":    ["trip_distance"],
    "fare":    ["fare_amount"],
    "pax":     ["passenger_count"],
    "pay":     ["payment_type"],
}
OPTIONAL = {"pax", "pay"}

def pick(df, key):
    for n in COLS[key]:
        if n in df.columns:
            return df[n]
    if key in OPTIONAL:
        return pd.Series(0, index=df.index)
    raise KeyError(f"No column for '{key}' in {list(df.columns)}")

def build_segment(cfg):
    """Cleaned, sorted taxi segment. Cached in data/ so the big parquet is read only once."""
    import pyarrow.parquet as pq
    path = resolve(cfg["parquet_path"])
    seg = resolve(f"data/segment_{path.stem}_{cfg['max_events']}.parquet")
    if seg.exists():
        return pd.read_parquet(seg)

    names = pq.ParquetFile(path).schema.names
    cols = []
    for key, cands in COLS.items():                 # read only the columns we need
        for n in cands:
            if n in names:
                cols.append(n)
                break
    df = pd.read_parquet(path, columns=cols)
    d = pd.DataFrame({
        "pickup_dt":  pd.to_datetime(pick(df, "pickup"), errors="coerce"),
        "dropoff_dt": pd.to_datetime(pick(df, "dropoff"), errors="coerce"),
        "pz":   pd.to_numeric(pick(df, "pu"), errors="coerce"),
        "dz":   pd.to_numeric(pick(df, "do"), errors="coerce"),
        "dist": pd.to_numeric(pick(df, "dist"), errors="coerce"),
        "fare": pd.to_numeric(pick(df, "fare"), errors="coerce"),
        "pax":  pd.to_numeric(pick(df, "pax"), errors="coerce").fillna(0),
        "pay":  pd.to_numeric(pick(df, "pay"), errors="coerce").fillna(0),
    })
    del df
    d = d.dropna(subset=["pickup_dt", "dropoff_dt", "pz", "dz", "dist", "fare"])
    d = d[(d.dropoff_dt > d.pickup_dt) & (d.fare >= 0) & (d.dist >= 0)]
    ym = d.pickup_dt.dt.to_period("M")              # keep the dominant month only
    d = d[ym == ym.mode()[0]]
    d = d.sort_values("pickup_dt", kind="stable").reset_index(drop=True).iloc[: cfg["max_events"]]
    d.to_parquet(seg)
    return d

def load_events(cfg):
    d = build_segment(cfg)
    month = d.pickup_dt.iloc[0].strftime("%Y-%m")
    fmt = "%Y-%m-%dT%H:%M:%S"
    events = []
    for i, r in enumerate(d.itertuples(index=False)):
        events.append({
            "event_id": f"{month}-row-{i:08d}",
            "event_time": r.pickup_dt.strftime(fmt),
            "dropoff_time": r.dropoff_dt.strftime(fmt),
            "pickup_zone": int(r.pz),
            "dropoff_zone": int(r.dz),
            "trip_distance": float(r.dist),
            "fare_amount": float(r.fare),
            "passenger_count": int(r.pax),
            "payment_type": int(r.pay),
        })
    return events
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float)
    ap.add_argument("--max-events", type=int)
    ap.add_argument("--key-mode", choices=["pickup_zone", "none"])
    ap.add_argument("--run-id")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--prepare-only", action="store_true", help="build the cached segment and exit")    
    a = ap.parse_args()

    cfg = load_config()
    rate = a.rate or cfg["producer_rate"]
    if a.max_events: cfg["max_events"] = a.max_events
    key_mode = a.key_mode or cfg["key_mode"]
    run_id = a.run_id or cfg["run_id"]
    topic = cfg["topics"]["raw"]
    dup_rate, late_rate, max_delay = cfg["duplicate_rate"], cfg["delayed_event_rate"], cfg["max_delay_seconds"]
    rng = random.Random(a.seed)

    events = load_events(cfg)
    if a.prepare_only:
        print(f"[producer] segment ready: {len(events)} events")
        return
    print(f"[producer] {len(events)} events, rate={rate}/s, key_mode={key_mode}, run_id={run_id}")

    part_counts = {}
    def on_delivery(err, msg):
        if err is None:
            part_counts[msg.partition()] = part_counts.get(msg.partition(), 0) + 1

    producer = Producer({"bootstrap.servers": cfg["bootstrap"], "linger.ms": 5,
                         "compression.type": "lz4", "queue.buffering.max.messages": 500000})

    t0 = time.time()
    sent = 0
    n_dup = n_late = 0

    def send(ev):
        nonlocal sent
        target = t0 + sent / rate                 # pace on ALL messages sent
        wait = target - time.time()
        if wait > 0:
            time.sleep(wait)
        ev = dict(ev)
        ev["producer_ts_ms"] = int(time.time() * 1000)
        key = str(ev["pickup_zone"]) if key_mode == "pickup_zone" else None
        while True:
            try:
                producer.produce(topic, key=key, value=json.dumps(ev), on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        sent += 1

    heap, seq, last_print = [], 0, time.time()

    def flush_due():
        while heap and heap[0][0] <= time.time():
            _, _, ev = heapq.heappop(heap)
            send(ev)

    for ev in events:
        flush_due()
        if rng.random() < late_rate:               # delayed: sent 1..max_delay s later
            seq += 1
            heapq.heappush(heap, (time.time() + rng.uniform(1, max_delay), seq, ev))
            n_late += 1
        else:
            send(ev)
            if rng.random() < dup_rate:            # duplicate: same event_id, resent shortly after
                seq += 1
                heapq.heappush(heap, (time.time() + rng.uniform(0.5, 5), seq, ev))
                n_dup += 1
        if time.time() - last_print > 5:
            print(f"[producer] sent={sent} actual={sent/(time.time()-t0):.0f}/s")
            last_print = time.time()


    main_elapsed = time.time() - t0
    main_sent = sent
    while heap:                                    # drain remaining delayed/dup events
        wait = heap[0][0] - time.time()
        if wait > 0:
            time.sleep(min(wait, 0.2))
        flush_due()

    producer.flush()
    elapsed = time.time() - t0
    total = sum(part_counts.values()) or 1
    stats = {
        "run_id": run_id, "requested_rate": rate, "actual_eps": round(main_sent / main_elapsed, 1), "drain_s": round(elapsed - main_elapsed, 2),
        "duplicates_sent": n_dup, "delayed_events": n_late,
        "key_mode": key_mode, "partition_counts": part_counts,
        "partition_skew_max_fraction": round(max(part_counts.values()) / total, 4),
        "elapsed_s": round(elapsed, 2),
    }
    (ROOT / "results").mkdir(exist_ok=True)
    with open(ROOT / "results" / f"producer_stats_{run_id}.json", "w") as f:
        json.dump(stats, f, indent=2)
    print("[producer] DONE", json.dumps(stats, indent=2))

if __name__ == "__main__":
    main()