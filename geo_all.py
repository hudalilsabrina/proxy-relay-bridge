"""Label geo massal: ukur negara keluar tiap proxy di pool, tulis countries.json."""
import json
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/opt/mrscraper-relay")
import chain

BASE = "/opt/mrscraper-relay"
POOL = os.path.join(BASE, "proxies.txt")
GEO = os.path.join(BASE, "countries.json")
WORKERS = int(os.getenv("GEO_WORKERS", "12"))
TIMEOUT = int(os.getenv("GEO_TIMEOUT", "12"))

pool = [l.strip() for l in open(POOL) if l.strip() and not l.startswith("#")]
print(f"[geo] {len(pool)} proxy, workers={WORKERS}", flush=True)


def one(p):
    cc, ip = chain.geo(p, timeout=TIMEOUT)
    return p, cc


found = {}
t0 = time.time()
with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    for i, (p, cc) in enumerate(ex.map(one, pool), 1):
        if cc:
            found[p] = cc
        if i % 500 == 0:
            el = time.time() - t0
            print(f"[geo] {i}/{len(pool)} labeled={len(found)} ({i/el:.1f}/s, sisa ~{(len(pool)-i)/(i/el)/60:.0f} menit)", flush=True)

tmp = GEO + ".tmp"
with open(tmp, "w") as f:
    json.dump(found, f, indent=0)
os.replace(tmp, GEO)

# relay hanya reload countries.json saat proxies.txt berubah -> sentuh filenya
try:
    os.utime(POOL, None)
except Exception:
    pass

c = Counter(found.values())
print(json.dumps({"ok": True, "labeled": len(found), "total": len(pool),
                  "countries": len(c), "top": dict(c.most_common(15))}), flush=True)
