"""Validasi batch besar kredensial FloppyData lalu push yang hidup ke relay."""
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/opt/mrscraper-relay")
import chain

SRC = sys.argv[1] if len(sys.argv) > 1 else "/root/.hermes/cache/documents/native-inputs/5a1dc37e6ec928fee7c96044f60696049b128fe61bbd9e467779685a3eda13fa/doc_da8275517a82_bansos 1000gb.txt"
TAG = os.getenv("BULK_TAG", "gologin")
WORKERS = int(os.getenv("BULK_WORKERS", "12"))
TIMEOUT = int(os.getenv("BULK_TIMEOUT", "12"))
OUT = "/opt/mrscraper-relay/bansos/bulk_alive.txt"


def norm(line):
    p = line.split(":")
    if len(p) == 4:
        h, port, u, pw = p
        return f"http://{u}:{pw}@{h}:{port}"
    return line if "://" in line else f"http://{line}"


def probe(u):
    ok, ip, err = chain.check(u, timeout=TIMEOUT)
    return u, ok


lines = [l.strip() for l in open(SRC) if l.strip() and not l.startswith("#")]
urls = sorted({norm(l) for l in lines})
print(f"[bulk] {len(lines)} baris -> {len(urls)} unik | workers={WORKERS} chain={'ON' if chain.PARENT_PROXY else 'OFF'}", flush=True)

alive = []
t0 = time.time()
with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    for i, (u, ok) in enumerate(ex.map(probe, urls), 1):
        if ok:
            alive.append(u)
        if i % 250 == 0:
            el = time.time() - t0
            print(f"[bulk] {i}/{len(urls)} hidup={len(alive)} ({i/el:.1f}/s, sisa ~{(len(urls)-i)/(i/el)/60:.0f} menit)", flush=True)

os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, "w") as f:
    f.write("\n".join(alive) + ("\n" if alive else ""))
print(f"[bulk] selesai: {len(alive)} hidup / {len(urls)} -> {OUT}", flush=True)

if alive:
    key = open("/opt/mrscraper-relay/ingest.key").read().strip()
    body = json.dumps({"proxies": alive, "tag": TAG, "mode": "append",
                       "validate": False, "geo": False}).encode()
    req = urllib.request.Request("http://127.0.0.1:8080/ingest", data=body,
                                 headers={"X-Api-Key": key, "Content-Type": "application/json"})
    print("[bulk] push:", urllib.request.urlopen(req, timeout=600).read().decode(), flush=True)
