import argparse, time
from confluent_kafka.admin import AdminClient, NewTopic
from common.config import load_config

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--partitions", type=int, help="partitions for taxi.raw and taxi.clean")
    ap.add_argument("--recreate", action="store_true", help="delete topics first")
    a = ap.parse_args()

    cfg = load_config()
    n = a.partitions or cfg["partitions"]
    t = cfg["topics"]
    plan = {t["raw"]: n, t["clean"]: n, t["metrics"]: 1, t["lsh"]: 1}

    admin = AdminClient({"bootstrap.servers": cfg["bootstrap"]})
    existing = set(admin.list_topics(timeout=10).topics)

    if a.recreate:
        todel = [x for x in plan if x in existing]
        if todel:
            for name, f in admin.delete_topics(todel, operation_timeout=30).items():
                f.result()
            for _ in range(30):
                time.sleep(1)
                if not (set(admin.list_topics(timeout=10).topics) & set(todel)):
                    break
        existing = set(admin.list_topics(timeout=10).topics)

    new = [NewTopic(x, num_partitions=p, replication_factor=1)
           for x, p in plan.items() if x not in existing]
    for name, f in admin.create_topics(new).items():
        try:
            f.result()
            print(f"created {name} ({plan[name]} partitions)")
        except Exception as e:
            print(f"{name}: {e}")
    print("topics now:", sorted(x for x in admin.list_topics(timeout=10).topics if x.startswith("taxi.")))

if __name__ == "__main__":
    main()