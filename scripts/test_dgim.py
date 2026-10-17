import random
from common.sketches import DGIM

random.seed(1)
bits = [1 if random.random() < 0.3 else 0 for _ in range(60000)]
for r in (2, 4, 8):
    d = DGIM(10000, r)
    for b in bits:
        d.add(b)
    out = []
    for k in (1000, 5000, 10000):
        ex = sum(bits[-k:])
        out.append(f"k={k}: exact={ex} est={d.query(k)}")
    print(f"r={r} buckets={d.num_buckets()} | " + " | ".join(out))