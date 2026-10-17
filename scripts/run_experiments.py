"""Runs the Task 4 experiment batch, one pipeline run after another (retries a failed run once)."""
import argparse, csv, subprocess, sys, time
from common.config import ROOT

EVENTS = 30000
P_RATE = 600          # above capacity, so differences in throughput/lag become visible

EXPERIMENTS = {
    # name:         partitions, slots, key,           rate
    "P1":          (1, 1, "pickup_zone", P_RATE),
    "P2":          (4, 1, "pickup_zone", P_RATE),
    "P3":          (4, 4, "pickup_zone", P_RATE),      # P3 is also K1 (key = pickup_zone)
    "P4":          (4, 6, "pickup_zone", P_RATE),
    "K2":          (4, 4, "none",        P_RATE),
    "rate-low":    (4, 4, "pickup_zone", 150),
    "rate-medium": (4, 4, "pickup_zone", 400),
    "rate-high":   (4, 4, "pickup_zone", 900),
    # optional second samples of P1-P4 (to show run-to-run variation)
    "P1b":         (1, 1, "pickup_zone", P_RATE),
    "P2b":         (4, 1, "pickup_zone", P_RATE),
    "P3b":         (4, 4, "pickup_zone", P_RATE),
    "P4b":         (4, 6, "pickup_zone", P_RATE),
}
DEFAULT_BATCH = ["P1", "P2", "P3", "P4", "K2", "rate-low", "rate-medium", "rate-high"]

def done_runs():
    p = ROOT / "results" / "experiment_summary.csv"
    if not p.exists():
        return set()
    with open(p, newline="", encoding="utf-8") as f:
        return {r["run_id"] for r in csv.DictReader(f) if r.get("events_processed")}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated names, e.g. K2 or P1b,P2b,P3b,P4b")
    ap.add_argument("--force", action="store_true", help="rerun even if already in the summary")
    a = ap.parse_args()

    names = a.only.split(",") if a.only else DEFAULT_BATCH
    skipped, failed = [], []
    for name in names:
        if name not in EXPERIMENTS:
            print(f"unknown experiment {name}; choose from {list(EXPERIMENTS)}")
            sys.exit(1)
        if name in done_runs() and not a.force:
            skipped.append(name)
            continue
        parts, slots, key, rate = EXPERIMENTS[name]
        print(f"\n########## {name}: partitions={parts} slots={slots} key={key} rate={rate} ##########")
        r = None
        for attempt in (1, 2):
            t0 = time.time()
            r = subprocess.run([sys.executable, "-m", "scripts.run_pipeline", "--run-id", name,
                                "--partitions", str(parts), "--slots", str(slots), "--rate", str(rate),
                                "--key-mode", key, "--max-events", str(EVENTS), "--idle", "30"],
                               cwd=str(ROOT))
            print(f"########## {name} attempt {attempt} finished in {(time.time() - t0) / 60:.1f} min, "
                  f"exit code {r.returncode}")
            if r.returncode == 0:
                break
            print(f"!! {name} failed (attempt {attempt}); " + ("retrying in 30 s" if attempt == 1 else "giving up"))
            time.sleep(30)
        if r.returncode != 0:
            failed.append(name)
        time.sleep(10)                      # let Java/Docker settle between runs

    print("\n==== batch finished ====")
    print("skipped (already done):", skipped)
    print("failed:", failed)
    print("see results\\experiment_summary.csv")

if __name__ == "__main__":
    main()