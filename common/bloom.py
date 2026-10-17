import math
import mmh3

def bloom_params(n, p):
    m = int(math.ceil(-n * math.log(p) / (math.log(2) ** 2)))
    k = max(1, int(round((m / n) * math.log(2))))
    return m, k

class BloomFilter:
    def __init__(self, m, k):
        self.m, self.k = m, k
        self.bits = bytearray((m + 7) // 8)

    def _positions(self, item):
        h1 = mmh3.hash(item, 0, signed=False)
        h2 = mmh3.hash(item, 1, signed=False) | 1
        return [(h1 + i * h2) % self.m for i in range(self.k)]

    def contains(self, item):
        return all(self.bits[p >> 3] & (1 << (p & 7)) for p in self._positions(item))

    def add(self, item):
        for p in self._positions(item):
            self.bits[p >> 3] |= 1 << (p & 7)

def make_bloom(cfg):
    m, k = bloom_params(cfg["bloom_expected_items"], cfg["bloom_false_positive_target"])
    if cfg.get("bloom_bits"):
        m = int(cfg["bloom_bits"])
    return BloomFilter(m, k)