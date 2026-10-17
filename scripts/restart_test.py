"""Task 5.2: stop Stage 2 while the producer runs, restart it, and record what survives."""
import argparse, json, subprocess, sys, time
from collections import Counter
from confluent_kafka import Consumer, TopicPartition
from common.config import ROOT, load_config
from scripts.run_pipeline import kill, spawn, wait_ready

RID = "restart"

def jl(name):
    p = ROOT / "results" / name
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]

def ckpt_state(topic):
    """Offsets Stage 2 has committed, read from the Spark checkpoint folder."""
    base = ROOT / "checkpoints" / f"stage2_{RID}"
    def ids(sub):
        d = base / sub
        return sorted(int(p.name) for p in d.iterdir() if p.name.isdigit()) if d.exists() else []
    try:
        offs, commits = ids("offsets"), ids("commits")
        if not commits:
            return None
        last = commits[-1]
        lines = [l for l in (base / "offsets" / str(last)).read_text().splitlines() if l.strip()]
        return {"committed_batch": last,
                "committed_offsets": json.loads(lines[-1]).get(topic, {}),
                "planned_not_committed": offs[-1] if offs and offs[-1] > last else None}
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=30000)
    ap.add_argument("--rate", type=float, default=200)
    ap.add_argument("--before", type=int, default=40, help="seconds of normal running before the kill")
    ap.add_argument("--downtime", type=int, default=30, help="seconds Stage 2 stays down")
    a = ap.parse_args()

    cfg = load_config()
    topic, nparts = cfg["topics"]["clean"], cfg["partitions"]
    for f in (ROOT / "results").glob(f"*_{RID}.json*"):
        f.unlink()

    subprocess.run([sys.executable, "-m", "producer.producer", "--prepare-only",
                    "--max-events", str(a.events)], cwd=str(ROOT), check=True)
    subprocess.run([sys.executable, "-m", "scripts.create_topics", "--recreate",
                    "--partitions", str(nparts)], cwd=str(ROOT), check=True)

    common = ["--run-id", RID, "--slots", "4", "--idle-timeout", "30"]
    procs = {"consumer": spawn("consumer", ["final_consumer.consumer", "--run-id", RID], RID),
             "stage1": spawn("stage1", ["stage1_clean_bloom.stage1", *common], RID)}
    time.sleep(6)
    procs["stage2"] = spawn("stage2", ["stage2_stream_analytics.stage2", *common], RID)
    for n in ("stage1", "stage2"):
        if not wait_ready(RID, n, f"[{n}] running", procs):
            print(f"!! {n} failed to start, see logs\\{RID}_{n}.log")
            for p in procs.values():
                kill(p)
            sys.exit(1)

    lc = Consumer({"bootstrap.servers": cfg["bootstrap"], "group.id": "restart-probe",
                   "enable.auto.commit": False})

    def lag_now():
        s = ckpt_state(topic)
        ends = {p: lc.get_watermark_offsets(TopicPartition(topic, p), timeout=5, cached=False)[1]
                for p in range(nparts)}
        if not s:
            return None, None, ends
        lag = sum(max(0, ends[p] - int(s["committed_offsets"].get(str(p), 0))) for p in range(nparts))
        return lag, s, ends

    out = {}
    prod = spawn("producer", ["producer.producer", "--run-id", RID, "--rate", str(a.rate),
                              "--key-mode", "pickup_zone", "--max-events", str(a.events)], RID)
    print(f"producer running at {a.rate}/s; Stage 2 will be killed in {a.before} s")
    time.sleep(a.before)

    # ---- BEFORE the kill ----
    lag_b, s_b, _ = lag_now()
    rows_before = sum(x["rows"] for x in jl(f"stage2_batches_{RID}.jsonl"))
    out["before_kill"] = {"lag": lag_b, "events_processed_by_stage2": rows_before,
                          "committed_batch": (s_b or {}).get("committed_batch"),
                          "committed_offsets": (s_b or {}).get("committed_offsets")}
    print("BEFORE KILL:", json.dumps(out["before_kill"]))

    # ---- KILL (hard kill, like a crash) ----
    kill(procs["stage2"])
    t_kill = time.time()
    time.sleep(2)
    s_k = ckpt_state(topic) or {}
    out["at_kill"] = {"committed_batch": s_k.get("committed_batch"),
                      "planned_not_committed_batch": s_k.get("planned_not_committed")}
    print("STAGE 2 KILLED:", json.dumps(out["at_kill"]))
    time.sleep(a.downtime)

    # ---- during downtime ----
    lag_d, s_d, ends_d = lag_now()
    out["during_downtime"] = {"seconds": a.downtime, "lag": lag_d,
                              "committed_offsets_unchanged": (s_d or {}).get("committed_offsets") ==
                                                              (s_b or {}).get("committed_offsets")
                                                              or "see before/after",
                              "end_offsets": ends_d}
    print("DURING DOWNTIME: lag =", lag_d)

    # ---- RESTART with the same checkpoint ----
    procs["stage2b"] = spawn("stage2b", ["stage2_stream_analytics.stage2", *common, "--resume"], RID)
    if not wait_ready(RID, "stage2b", "[stage2] running", procs):
        print("!! restarted Stage 2 failed, see logs\\restart_stage2b.log")
        for p in list(procs.values()) + [prod]:
            kill(p)
        sys.exit(1)
    t_restart = time.time()
    print("STAGE 2 RESTARTED, watching the lag drain...")
    timeline, catch_up = [], None
    while time.time() - t_restart < 150:
        l, s, _ = lag_now()
        if l is not None:
            timeline.append([round(time.time() - t_restart, 1), l])
            if l < 500 and time.time() - t_restart > 10:
                catch_up = round(time.time() - t_restart, 1)
                break
        time.sleep(3)
    out["after_restart"] = {"lag_timeline_[seconds_since_restart, lag]": timeline,
                            "seconds_to_catch_up": catch_up}
    print("LAG TIMELINE:", timeline)

    # ---- let everything finish ----
    t0 = time.time()
    while (procs["stage1"].poll() is None or procs["stage2b"].poll() is None or prod.poll() is None) \
            and time.time() - t0 < 400:
        time.sleep(3)
    time.sleep(8)
    _, _, ends_final = lag_now()
    for p in list(procs.values()) + [prod]:
        kill(p)

    # ---- analysis ----
    batches = jl(f"stage2_batches_{RID}.jsonl")
    after = [x for x in batches if x["ts_ms"] > t_kill * 1000]
    first_after = after[0]["batch_id"] if after else None
    ids = Counter(x["batch_id"] for x in batches)
    total_rows, total_clean = sum(x["rows"] for x in batches), sum(ends_final.values())
    series = [r["events_processed"] for r in jl(f"results_{RID}.jsonl")]
    drops = [[series[i - 1], series[i]] for i in range(1, len(series)) if series[i] < series[i - 1]]
    out["offsets_and_duplicates"] = {
        "planned_not_committed_batch_at_kill": s_k.get("planned_not_committed"),
        "first_batch_id_after_restart": first_after,
        "batch_ids_logged_twice": [i for i, c in ids.items() if c > 1],
        "events_in_taxi_clean": total_clean,
        "events_processed_over_all_completed_batches": total_rows,
        "events_processed_twice": max(0, total_rows - total_clean)}
    out["state"] = {"events_processed_counter_drops_[before,after]": drops,
                    "note": "a drop means the sketches/windows restarted empty after the crash"}

    wins = [w for w in jl(f"windows_{RID}.jsonl") if w.get("type") == "window"]
    base = {w["window_start"]: w["events_in_window"] for w in jl("windows_P3.jsonl") if w.get("type") == "window"}
    cnt = Counter(w["window_start"] for w in wins)
    out["windows_vs_no_failure_run_P3"] = [
        {"window_start": w["window_start"], "events_after_crash_run": w["events_in_window"],
         "events_no_failure_P3": base.get(w["window_start"]), "times_emitted": cnt[w["window_start"]]}
        for w in wins]

    with open(ROOT / "results" / "restart_test.json", "w") as f:
        json.dump(out, f, indent=2)
    print("\n==== RESTART TEST RESULT ====")
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()