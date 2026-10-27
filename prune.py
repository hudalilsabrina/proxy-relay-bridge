#!/usr/bin/env python3
"""Prune pool: tes semua proxy di pool.d/*.txt, buang yang mati, rebuild proxies.txt.

Dijalankan periodik oleh relay-prune.timer (default tiap 10 menit).
Relay mendeteksi perubahan file dan hot-reload sendiri.
"""
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

BASE = "/opt/mrscraper-relay"
POOL_DIR = os.path.join(BASE, "pool.d")
POOL_FILE = os.path.join(BASE, "proxies.txt")
TIMEOUT = int(os.getenv("PRUNE_TIMEOUT", "20"))


def check(p):
    try:
        out = subprocess.run(["curl", "-s", "-m", str(TIMEOUT), "-x", p,
                              "http://api.ipify.org", "-w", "|%{http_code}"],
                             capture_output=True, text=True, timeout=TIMEOUT + 10).stdout
        body, _, code = out.partition("|")
        return p, code == "200" and bool(body.strip())
    except Exception:
        return p, False


def main():
    total = live_total = 0
    for fn in sorted(os.listdir(POOL_DIR)):
        if not fn.endswith(".txt"):
            continue
        path = os.path.join(POOL_DIR, fn)
        lines = [l.strip() for l in open(path) if l.strip() and not l.startswith("#")]
        if not lines:
            continue
        with ThreadPoolExecutor(max_workers=20) as ex:
            results = list(ex.map(check, lines))
        live = [p for p, ok in results if ok]
        total += len(lines)
        live_total += len(live)
        if len(live) != len(lines):
            with open(path, "w") as f:
                f.write("\n".join(live) + ("\n" if live else ""))
        print(f"{fn:24} {len(live):4}/{len(lines):<4} hidup", flush=True)

    seen, pool = set(), []
    for fn in sorted(os.listdir(POOL_DIR)):
        if fn.endswith(".txt"):
            for l in open(os.path.join(POOL_DIR, fn)):
                l = l.strip()
                if l and not l.startswith("#") and l not in seen:
                    seen.add(l)
                    pool.append(l)
    tmp = POOL_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write("\n".join(pool) + ("\n" if pool else ""))
    os.replace(tmp, POOL_FILE)
    print(json.dumps({"ok": True, "checked": total, "live": live_total, "pool_size": len(pool)}))


if __name__ == "__main__":
    main()
