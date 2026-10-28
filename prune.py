#!/usr/bin/env python3
"""Prune pool: tes semua proxy di pool.d/*.txt, buang yang mati, rebuild proxies.txt.

Dijalankan periodik oleh relay-prune.timer (default tiap 10 menit).
Relay mendeteksi perubahan file dan hot-reload sendiri.

PENGAMAN (penting untuk gateway residential seperti geo.floppydata.com):
- worker sedikit (default 5) karena gateway men-rate-limit kalau ditembak paralel
- tiap proxy dicoba 2x; baru dianggap mati kalau DUA-duanya gagal
- kalau >70% pool tiba-tiba mati, prune DIBATALKAN dan file tidak ditimpa
  (indikasi rate-limit gateway / gangguan jaringan lokal, bukan proxy mati)
"""
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

import chain

BASE = "/opt/mrscraper-relay"
POOL_DIR = os.path.join(BASE, "pool.d")
POOL_FILE = os.path.join(BASE, "proxies.txt")
GEO_FILE = os.path.join(BASE, "countries.json")
TIMEOUT = int(os.getenv("PRUNE_TIMEOUT", "20"))
WORKERS = int(os.getenv("PRUNE_WORKERS", "5"))
MIN_ALIVE_RATIO = float(os.getenv("PRUNE_MIN_ALIVE_RATIO", "0.3"))


def _attempt(p):
    if chain.PARENT_PROXY:
        ok, _ip, _err = chain.check(p, timeout=TIMEOUT)
        return ok
    try:
        out = subprocess.run(["curl", "-s", "-m", str(TIMEOUT), "-x", p,
                              "http://api.ipify.org", "-w", "|%{http_code}"],
                             capture_output=True, text=True, timeout=TIMEOUT + 10).stdout
        body, _, code = out.partition("|")
        return code == "200" and bool(body.strip())
    except Exception:
        return False


def check(p):
    """Dua percobaan; baru mati kalau dua-duanya gagal."""
    if _attempt(p):
        return p, True
    return p, _attempt(p)


def refresh_geo(pool):
    """Ukur ulang negara keluar tiap proxy hidup, lalu tulis countries.json
    dengan key = URL proxy (format yang dibaca relay.py)."""
    def one(p):
        if chain.PARENT_PROXY:
            cc, _ip = chain.geo(p, timeout=TIMEOUT)
            return p, cc
        try:
            out = subprocess.run(["curl", "-s", "-m", str(TIMEOUT), "-x", p,
                                  "https://ipinfo.io/json"],
                                 capture_output=True, text=True, timeout=TIMEOUT + 8).stdout
            d = json.loads(out) if out.strip().startswith("{") else {}
            return p, (d.get("country") or "").lower()
        except Exception:
            return p, ""

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        found = {p: cc for p, cc in ex.map(one, pool) if cc}
    if not found:
        return 0
    tmp = GEO_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(found, f, indent=0)
    os.replace(tmp, GEO_FILE)
    return len(found)


def main():
    total = live_total = 0
    abort = False

    for fn in sorted(os.listdir(POOL_DIR)):
        if not fn.endswith(".txt"):
            continue
        path = os.path.join(POOL_DIR, fn)
        lines = [l.strip() for l in open(path) if l.strip() and not l.startswith("#")]
        if not lines:
            continue

        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            results = list(ex.map(check, lines))
        live = [p for p, ok in results if ok]

        if len(lines) >= 10 and len(live) / len(lines) < MIN_ALIVE_RATIO:
            print(f"ABORT {fn}: hanya {len(live)}/{len(lines)} hidup "
                  f"(<{MIN_ALIVE_RATIO:.0%}) — kemungkinan gateway rate-limit, "
                  f"file TIDAK diubah", flush=True)
            abort = True
            break

        total += len(lines)
        live_total += len(live)
        if len(live) != len(lines):
            with open(path, "w") as f:
                f.write("\n".join(live) + ("\n" if live else ""))
        print(f"{fn:24} {len(live):4}/{len(lines):<4} hidup", flush=True)

    if abort:
        print(json.dumps({"ok": False, "aborted": True, "reason": "mass failure (rate-limit?)"}))
        return

    seen, pool = set(), []
    for fn in sorted(os.listdir(POOL_DIR)):
        if fn.endswith(".txt"):
            for l in open(os.path.join(POOL_DIR, fn)):
                l = l.strip()
                if l and not l.startswith("#") and l not in seen:
                    seen.add(l)
                    pool.append(l)

    geo_n = refresh_geo(pool)          # tulis peta negara DULU, baru proxies.txt
    tmp = POOL_FILE + ".tmp"           # supaya hot-reload relay membaca keduanya segar
    with open(tmp, "w") as f:
        f.write("\n".join(pool) + ("\n" if pool else ""))
    os.replace(tmp, POOL_FILE)
    print(json.dumps({"ok": True, "checked": total, "live": live_total,
                      "pool_size": len(pool), "geo": geo_n}))


if __name__ == "__main__":
    main()
