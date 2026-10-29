"""Sampel cepat: ukur berapa persen kredensial di file yang masih hidup."""
import random
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/opt/mrscraper-relay")
import chain

SRC = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 40
W = int(sys.argv[3]) if len(sys.argv) > 3 else 8


def norm(line):
    if "://" in line:
        return line
    p = line.split(":")
    if len(p) == 4:
        h, port, u, pw = p
        return f"http://{u}:{pw}@{h}:{port}"
    return f"http://{line}"


lines = [l.strip() for l in open(SRC) if l.strip()]
urls = sorted({norm(l) for l in lines})
print(f"total {len(lines)} baris, {len(urls)} unik, chain={'ON' if chain.PARENT_PROXY else 'OFF'}", flush=True)

sample = random.sample(urls, min(N, len(urls)))
with ThreadPoolExecutor(max_workers=W) as ex:
    res = list(ex.map(lambda u: chain.check(u, timeout=15)[0], sample))
alive = sum(res)
print(f"sampel {len(sample)} -> hidup {alive} ({alive/len(sample)*100:.0f}%)", flush=True)
print(f"estimasi hidup total: {round(alive/len(sample)*len(urls))}", flush=True)
