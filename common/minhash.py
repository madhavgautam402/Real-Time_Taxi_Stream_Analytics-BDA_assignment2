from collections import defaultdict
from itertools import combinations
import numpy as np

P = (1 << 31) - 1          # prime; zone IDs are small so int64 never overflows

class MinHasher:
    def __init__(self, num_hashes=100, seed=7):
        rng = np.random.RandomState(seed)
        self.n = num_hashes
        self.a = rng.randint(1, P, size=num_hashes).astype(np.int64)
        self.b = rng.randint(0, P, size=num_hashes).astype(np.int64)

    def signature(self, items):
        x = np.fromiter(items, dtype=np.int64)
        h = (self.a[:, None] * x[None, :] + self.b[:, None]) % P
        return h.min(axis=1)

def lsh_candidates(sigs, bands, rows):
    """sigs: {zone: signature array}. Returns set of (z1, z2), z1 < z2, sharing >=1 band bucket."""
    cands = set()
    for bnd in range(bands):
        lo, hi = bnd * rows, (bnd + 1) * rows
        buckets = defaultdict(list)
        for z, s in sigs.items():
            buckets[s[lo:hi].tobytes()].append(z)
        for members in buckets.values():
            if len(members) > 1:
                members.sort()
                cands.update(combinations(members, 2))
    return cands

def jaccard(a, b):
    inter = len(a & b)
    union = len(a) + len(b) - inter
    return inter / union if union else 0.0