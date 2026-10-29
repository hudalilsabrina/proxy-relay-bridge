"""Push seluruh isi file ke relay tanpa validasi (cepat, untuk batch besar)."""
import json
import sys
import urllib.request

SRC = sys.argv[1]
TAG = sys.argv[2] if len(sys.argv) > 2 else "public"
KEY = open("/opt/mrscraper-relay/ingest.key").read().strip()


def norm(line):
    if "://" in line:
        return line
    p = line.split(":")
    if len(p) == 4:
        h, port, u, pw = p
        return f"http://{u}:{pw}@{h}:{port}"
    if len(p) == 2:
        return f"http://{line}"
    return line


urls = sorted({norm(l.strip()) for l in open(SRC) if l.strip() and not l.startswith("#")})
print(f"{len(urls)} proxy unik dari {SRC} -> tag {TAG}", flush=True)

CHUNK = 5000
total = {"accepted": 0, "duplicate": 0, "bad_format": 0, "pool_size": 0}
for i in range(0, len(urls), CHUNK):
    part = urls[i:i + CHUNK]
    body = json.dumps({"proxies": part, "tag": TAG, "mode": "append",
                       "validate": False, "geo": False}).encode()
    req = urllib.request.Request("http://127.0.0.1:8080/ingest", data=body,
                                 headers={"X-Api-Key": KEY, "Content-Type": "application/json"})
    r = json.loads(urllib.request.urlopen(req, timeout=600).read().decode())
    for k in total:
        total[k] = r.get(k, total[k])
    print(f"  chunk {i//CHUNK+1}: +{r.get('accepted',0)} (pool {r.get('pool_size')})", flush=True)

print(json.dumps(total), flush=True)
