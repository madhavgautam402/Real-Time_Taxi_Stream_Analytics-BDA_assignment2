import random
from common.sketches import HyperLogLog, DGIM, Reservoir

# HLL
h = HyperLogLog(12)
for i in range(50000):
    h.add(f"route-{i}")
print("HLL exact=50000 est=", h.count(), "mem bytes:", h.memory_bytes())

# DGIM
random.seed(1)
N = 10000
d = DGIM(N)
bits = []
for _ in range(60000):
    b = 1 if random.random() < 0.3 else 0
    d.add(b); bits.append(b)
for k in (1000, 5000, 10000):
    ex = sum(bits[-k:])
    print(f"DGIM k={k} exact={ex} est={d.query(k)} buckets={d.num_buckets()}")

# Reservoir
r = Reservoir(1000, 1)
vals = [random.gauss(18, 5) for _ in range(100000)]
for v in vals: r.add(v)
print("Reservoir mean exact=%.3f est=%.3f" % (sum(vals)/len(vals), sum(r.items)/len(r.items)))