#!/usr/bin/env python3
"""Bansos: hasilkan file .txt berisi proxy FloppyData yang masih hidup,
siap dibagikan manual oleh bot ke user.

Sumber: semua kredensial hasil panen (data/proxies.json) + pool relay aktif.
Kredensial divalidasi dulu (lewat PARENT_PROXY kalau IP server diblokir gateway),
jadi yang masuk bansos.txt benar-benar hidup saat file dibuat.

Output di /opt/mrscraper-relay/bansos/:
  bansos.txt          -> baris polos siap kirim: http://user:pass@host:port
  bansos_grouped.txt  -> dikelompokkan per negara (ada komentar #)
  bansos.json         -> [{country, proxy, host, port}] untuk bot yang butuh filter
  bansos_meta.json    -> jumlah, waktu generate, sebaran negara
"""
import json
import os
import subprocess
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import chain

BASE = "/opt/mrscraper-relay"
OUT = os.path.join(BASE, "bansos")
SRC_JSON = "/root/gologin-suite/data/proxies.json"
POOL_FILE = os.path.join(BASE, "proxies.txt")
WORKERS = int(os.getenv("BANSOS_WORKERS", "6"))
TIMEOUT = int(os.getenv("BANSOS_TIMEOUT", "15"))


def load_candidates():
    """Semua kredensial unik: hasil panen + pool aktif."""
    creds = {}
    try:
        data = json.load(open(SRC_JSON))
        for x in data:
            u = x.get("formatted")
            if u:
                creds[u] = (x.get("country") or "").upper()
    except Exception:
        pass
    try:
        for line in open(POOL_FILE):
            line = line.strip()
            if line and not line.startswith("#"):
                creds.setdefault(line, "")
    except Exception:
        pass
    return creds


def probe(item):
    url, cc = item
    ok, ip, err = chain.check(url, timeout=TIMEOUT)
    if not ok:
        return None
    if not cc or cc == "ALL":
        g, _ = chain.geo(url, timeout=TIMEOUT)
        cc = (g or "").upper()
    return {"proxy": url, "country": cc, "exit_ip": ip}


def main():
    os.makedirs(OUT, exist_ok=True)
    creds = load_candidates()
    print(f"[bansos] kandidat: {len(creds)} kredensial (chain={'ON' if chain.PARENT_PROXY else 'OFF'})", flush=True)

    alive = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for i, r in enumerate(ex.map(probe, creds.items()), 1):
            if r:
                alive.append(r)
            if i % 50 == 0:
                print(f"[bansos] {i}/{len(creds)} — hidup {len(alive)}", flush=True)

    alive.sort(key=lambda x: (x["country"] or "ZZ", x["proxy"]))

    # 1) file polos untuk dikirim bot
    with open(os.path.join(OUT, "bansos.txt"), "w") as f:
        f.write("\n".join(a["proxy"] for a in alive) + ("\n" if alive else ""))

    # 2) dikelompokkan per negara
    grouped = Counter(a["country"] for a in alive)
    with open(os.path.join(OUT, "bansos_grouped.txt"), "w") as f:
        f.write(f"# BANSOS PROXY — {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}\n")
        f.write(f"# total {len(alive)} proxy hidup dari {len(grouped)} negara\n")
        cur = None
        for a in alive:
            if a["country"] != cur:
                cur = a["country"]
                f.write(f"\n# --- {cur or 'UNKNOWN'} ({grouped[cur]}) ---\n")
            f.write(a["proxy"] + "\n")

    # 3) json + meta
    json.dump(alive, open(os.path.join(OUT, "bansos.json"), "w"), indent=1)
    meta = {
        "generated_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "candidates": len(creds),
        "alive": len(alive),
        "dead": len(creds) - len(alive),
        "countries": len(grouped),
        "top_countries": dict(grouped.most_common(10)),
    }
    json.dump(meta, open(os.path.join(OUT, "bansos_meta.json"), "w"), indent=1)
    print(json.dumps(meta), flush=True)


if __name__ == "__main__":
    main()
