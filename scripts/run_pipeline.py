"""One-command run: prepare data -> topics -> final consumer + stage1/2/3 -> producer -> wait -> summary."""
import argparse, json, os, subprocess, sys, time
import yaml
from common.config import ROOT, load_config
from scripts.summarize_run import summarize

LOGS = ROOT / "logs"

def log_path(run_id, name):
    return LOGS / f"{run_id}_{name}.log"

def spawn(name, mod_args, run_id):
    LOGS.mkdir(exist_ok=True)
    f = open(log_path(run_id, name), "w", encoding="utf-8")
    return subprocess.Popen([sys.executable, "-u", "-m", *mod_args], cwd=str(ROOT),
                            stdout=f, stderr=subprocess.STDOUT)

def kill(p):
    if p.poll() is None:
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], capture_output=True)

def tail(run_id, name, n=15):
    p = log_path(run_id, name)
    if not p.exists():
        return ""
    return "\n".join(p.read_text(encoding="utf-8", errors="ignore").splitlines()[-n:])

def wait_ready(run_id, name, marker, procs, timeout=150):
    t0 = time.time()
    while time.time() - t0 < timeout:
        p = log_path(run_id, name)
        if p.exists() and marker in p.read_text(encoding="utf-8", errors="ignore"):
            return True
        if procs[name].poll() is not None:
            return False
        time.sleep(2)
    return False

def last_line(run_id, name, key):
    p = log_path(run_id, name)
    if not p.exists():
        return ""
    lines = [l for l in p.read_text(encoding="utf-8", errors="ignore").splitlines() if key in l]
    return lines[-1] if lines else ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--partitions", type=int)
    ap.add_argument("--slots", type=int, default=4, help="Spark local[N] slots per stage")
    ap.add_argument("--rate", type=float)
    ap.add_argument("--key-mode", choices=["pickup_zone", "none"])
    ap.add_argument("--max-events", type=int)
    ap.add_argument("--idle", type=int, default=45, help="seconds without data before stages stop")
    ap.add_argument("--timeout", type=int, default=900, help="hard limit for the whole run (seconds)")
    ap.add_argument("--set", action="append", default=[], help="override a config key, e.g. --set bloom_bits=100000")
    a = ap.parse_args()

    overrides = {}
    for kv in a.set:
        k, v = kv.split("=", 1)
        overrides[k] = yaml.safe_load(v)
    if overrides:
        os.environ["BDA_OVERRIDES"] = json.dumps(overrides)   # inherited by every child process
        print("config overrides:", overrides)

    cfg = load_config()
    rid = a.run_id
    parts = a.partitions or cfg["partitions"]
    rate = a.rate or cfg["producer_rate"]
    key_mode = a.key_mode or cfg["key_mode"]
    maxev = a.max_events or cfg["max_events"]
    print(f"=== RUN {rid}: partitions={parts} slots={a.slots} key={key_mode} rate={rate} events={maxev} ===")

    for f in (ROOT / "results").glob(f"*_{rid}.json*"):      # clear this run's old outputs
        f.unlink()

    # 1) build the cached taxi segment BEFORE any Spark JVM is started (saves RAM)
    print("preparing taxi segment (first time takes ~30 s)...")
    r = subprocess.run([sys.executable, "-m", "producer.producer", "--prepare-only",
                        "--max-events", str(maxev)], cwd=str(ROOT))
    if r.returncode != 0:
        print("!! could not prepare the taxi segment")
        sys.exit(1)

    # 2) fresh topics
    subprocess.run([sys.executable, "-m", "scripts.create_topics", "--recreate", "--partitions", str(parts)],
                   cwd=str(ROOT), check=True)

    # 3) start consumer and stages
    common = ["--run-id", rid, "--slots", str(a.slots), "--idle-timeout", str(a.idle)]
    procs = {"consumer": spawn("consumer", ["final_consumer.consumer", "--run-id", rid], rid)}
    procs["stage1"] = spawn("stage1", ["stage1_clean_bloom.stage1", *common, "--key-mode", key_mode], rid)
    time.sleep(6)
    procs["stage2"] = spawn("stage2", ["stage2_stream_analytics.stage2", *common], rid)
    time.sleep(6)
    procs["stage3"] = spawn("stage3", ["stage3_lsh.stage3", *common], rid)

    for name, marker in (("stage1", "[stage1] running"), ("stage2", "[stage2] running"),
                         ("stage3", "[stage3] running")):
        if not wait_ready(rid, name, marker, procs):
            print(f"!! {name} failed to start. Last log lines:\n{tail(rid, name, 25)}")
            for p in procs.values():
                kill(p)
            sys.exit(1)
    print("stages are up, starting producer...")

    # 4) producer
    prod = spawn("producer", ["producer.producer", "--run-id", rid, "--rate", str(rate),
                              "--key-mode", key_mode, "--max-events", str(maxev)], rid)

    # 5) wait until producer and all stages are finished (fail fast on any crash)
    t0, last, crashed = time.time(), 0, False
    stage_names = ("stage1", "stage2", "stage3")
    while any(procs[n].poll() is None for n in stage_names) or prod.poll() is None:
        for n, p in list(procs.items()) + [("producer", prod)]:
            if p.poll() not in (None, 0):
                print(f"!! {n} exited with code {p.poll()}. Last log lines:\n{tail(rid, n, 15)}")
                crashed = True
        if crashed:
            break
        if time.time() - t0 > a.timeout:
            print("!! timeout, killing everything")
            break
        if time.time() - last > 15:
            last = time.time()
            print("  ", last_line(rid, "stage2", "REPORT")[:180] or "(waiting for data...)")
        time.sleep(2)

    time.sleep(8)                                    # let the final consumer read the last messages
    for p in list(procs.values()) + [prod]:
        kill(p)

    if crashed:
        print("run aborted because a process crashed; no summary row written")
        sys.exit(1)

    row = summarize(rid, partitions=parts, slots=a.slots, key_mode=key_mode, requested_rate=rate)
    print("=== DONE ===")
    for k, v in row.items():
        print(f"  {k}: {v}")
    print(f"logs: {LOGS}   results: {ROOT / 'results'}")

if __name__ == "__main__":
    main()