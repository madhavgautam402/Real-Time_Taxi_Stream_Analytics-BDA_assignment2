import math, random
import mmh3

class HyperLogLog:
    def __init__(self, p=12):
        self.p = p
        self.m = 1 << p
        self.reg = [0] * self.m
        self.alpha = 0.7213 / (1 + 1.079 / self.m)

    def add(self, item):
        h = mmh3.hash64(item, 0, signed=False)[0]
        idx = h >> (64 - self.p)
        w = h & ((1 << (64 - self.p)) - 1)
        rho = (64 - self.p) - w.bit_length() + 1
        if rho > self.reg[idx]:
            self.reg[idx] = rho

    def count(self):
        z = sum(2.0 ** -r for r in self.reg)
        e = self.alpha * self.m * self.m / z
        if e <= 2.5 * self.m:                       # small-range correction
            v = self.reg.count(0)
            if v:
                e = self.m * math.log(self.m / v)
        return int(round(e))

    def memory_bytes(self):                          # 6 bits per register
        return self.m * 6 // 8


class DGIM:
    """Buckets are [timestamp_of_most_recent_1, size], newest first."""
    def __init__(self, N, r=2):
        self.N = N
        self.r = r                       # max buckets allowed per size
        self.t = 0
        self.buckets = []

    def add(self, bit):
        self.t += 1
        while self.buckets and self.buckets[-1][0] <= self.t - self.N:
            self.buckets.pop()                       # expired from window
        if bit:
            self.buckets.insert(0, [self.t, 1])
            size = 1
            while True:
                idxs = [j for j, b in enumerate(self.buckets) if b[1] == size]
                if len(idxs) > self.r:       # merge the two OLDEST of this size                  # merge the two OLDEST of this size
                    newer, older = idxs[-2], idxs[-1]
                    self.buckets[newer][1] = size * 2
                    del self.buckets[older]
                    size *= 2
                else:
                    break

    def query(self, k):
        k = min(k, self.N)
        cutoff = self.t - k
        total, last = 0, 0
        for ts, size in self.buckets:
            if ts > cutoff:
                total += size
                last = size
            else:
                break
        return total - last // 2 if total else 0

    def num_buckets(self):
        return len(self.buckets)


class Reservoir:
    """Algorithm R."""
    def __init__(self, k, seed=0):
        self.k = k
        self.items = []
        self.n = 0
        self.rng = random.Random(seed)

    def add(self, x):
        self.n += 1
        if len(self.items) < self.k:
            self.items.append(x)
        else:
            j = self.rng.randrange(self.n)
            if j < self.k:
                self.items[j] = x