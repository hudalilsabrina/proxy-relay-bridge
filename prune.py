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
# Pool besar (puluhan ribu): cek sebagian saja tiap run, sisanya dibiarkan.
# 0 = cek semua.
SAMPLE = int(os.getenv("PRUNE_SAMPLE", "0"))


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
    """Label negara untuk proxy yang BELUM ada di countries.json saja,
    lalu gabungkan dengan peta lama (biar pool besar tidak dilabel ulang semua)."""
    try:
        cur = json.load(open(GEO_FILE)) or {}
    except Exception:
        cur = {}
    todo = [p for p in pool if p not in cur]

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
        found = {p: cc for p, cc in ex.map(one, todo) if cc}
    if not found:
        return 0
    found = {**cur, **found}
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

        # Pool besar: cek sampel saja, sisanya dibiarkan apa adanya.
        if SAMPLE and len(lines) > SAMPLE:
            check_list = random.sample(lines, SAMPLE)
            unchecked = [p for p in lines if p not in set(check_list)]
        else:
            check_list = lines
            unchecked = []

        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            results = list(ex.map(check, check_list))
        live_checked = [p for p, ok in results if ok]
        dead_n = len(check_list) - len(live_checked)

        if len(check_list) >= 10 and len(live_checked) / len(check_list) < MIN_ALIVE_RATIO:
            print(f"ABORT {fn}: hanya {len(live_checked)}/{len(check_list)} hidup "
                  f"(<{MIN_ALIVE_RATIO:.0%}) — kemungkinan gateway rate-limit, "
                  f"file TIDAK diubah", flush=True)
            abort = True
            break

        live = unchecked + live_checked
        total += len(check_list)
        live_total += len(live_checked)
        if dead_n:
            with open(path, "w") as f:
                f.write("\n".join(live) + ("\n" if live else ""))
        extra = f" (sampel {len(check_list)}/{len(lines)})" if unchecked else ""
        print(f"{fn:24} {len(live_checked):4}/{len(check_list):<4} hidup{extra}", flush=True)

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
