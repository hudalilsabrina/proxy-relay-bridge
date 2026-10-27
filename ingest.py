#!/usr/bin/env python3
"""Ingest API — terima proxy dari agent luar (GoLogin farmer, dsb) ke relay pool.

Endpoints (semua butuh header X-Api-Key kecuali /health):
  POST /ingest   body JSON {"proxies":["http://user:pass@host:port", ...],
                            "tag":"gologin", "mode":"append|replace", "validate":false}
                 atau text/plain satu proxy per baris
  GET  /health   ringkasan pool (tanpa auth)
  GET  /list     daftar proxy aktif
  POST /rebuild  bangun ulang proxies.txt dari pool.d/ lalu relay reload sendiri

Penyimpanan: /opt/mrscraper-relay/pool.d/<tag>.txt  (satu file per sumber)
Pool aktif : /opt/mrscraper-relay/proxies.txt       (union semua tag, dibaca relay)
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE = "/opt/mrscraper-relay"
POOL_DIR = os.path.join(BASE, "pool.d")
POOL_FILE = os.path.join(BASE, "proxies.txt")
KEY_FILE = os.path.join(BASE, "ingest.key")
GEO_FILE = os.path.join(BASE, "countries.json")
PORTS = [int(x) for x in os.getenv("INGEST_PORTS", os.getenv("INGEST_PORT", "8787,8080")).split(",") if x.strip()]

os.makedirs(POOL_DIR, exist_ok=True)


def api_key() -> str:
    if os.path.exists(KEY_FILE):
        return open(KEY_FILE).read().strip()
    k = os.urandom(16).hex()
    with open(KEY_FILE, "w") as f:
        f.write(k)
    os.chmod(KEY_FILE, 0o600)
    return k


KEY = api_key()
PROXY_RE = re.compile(r"^(https?|socks5h?)://[^\s@]+@[^\s@]+:\d+$")


def tag_path(tag: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_-]", "", tag or "default")[:32] or "default"
    return os.path.join(POOL_DIR, f"{safe}.txt")


def read_lines(path: str) -> list:
    try:
        return [l.strip() for l in open(path) if l.strip() and not l.startswith("#")]
    except Exception:
        return []


def all_tags() -> dict:
    out = {}
    for fn in sorted(os.listdir(POOL_DIR)):
        if fn.endswith(".txt"):
            out[fn[:-4]] = len(read_lines(os.path.join(POOL_DIR, fn)))
    return out


def rebuild() -> int:
    seen, pool = set(), []
    for fn in sorted(os.listdir(POOL_DIR)):
        if not fn.endswith(".txt"):
            continue
        for l in read_lines(os.path.join(POOL_DIR, fn)):
            if l not in seen:
                seen.add(l)
                pool.append(l)
    tmp = POOL_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write("\n".join(pool) + ("\n" if pool else ""))
    os.replace(tmp, POOL_FILE)
    return len(pool)


def geo_lookup(proxies: list, timeout: int = 20) -> dict:
    """Cari negara tiap proxy (dipakai filter country-xx di relay)."""
    def one(p):
        try:
            out = subprocess.run(["curl", "-s", "-m", str(timeout), "-x", p,
                                  "https://ipinfo.io/json"],
                                 capture_output=True, text=True, timeout=timeout + 8).stdout
            d = json.loads(out) if out.strip().startswith("{") else {}
            cc = (d.get("country") or "").lower()
            return p, cc, d.get("ip", "")
        except Exception:
            return p, "", ""

    found = {}
    with ThreadPoolExecutor(max_workers=15) as ex:
        for p, cc, _ip in ex.map(one, proxies):
            if cc:
                found[p] = cc
    return found


def save_geo(found: dict) -> int:
    try:
        cur = json.load(open(GEO_FILE))
    except Exception:
        cur = {}
    cur.update(found)
    tmp = GEO_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cur, f, indent=0)
    os.replace(tmp, GEO_FILE)
    return len(cur)


def validate(proxies: list, timeout: int = 20) -> list:
    def check(p):
        try:
            out = subprocess.run(["curl", "-s", "-m", str(timeout), "-x", p,
                                  "http://api.ipify.org", "-w", "|%{http_code}"],
                                 capture_output=True, text=True, timeout=timeout + 8).stdout
            body, _, code = out.partition("|")
            return p if code == "200" and body.strip() else None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=20) as ex:
        return [p for p in ex.map(check, proxies) if p]


class Handler(BaseHTTPRequestHandler):
    server_version = "RelayIngest/1.0"

    def _send(self, code: int, obj: dict):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authed(self) -> bool:
        return self.headers.get("X-Api-Key", "") == KEY

    def log_message(self, fmt, *args):
        print(f"[ingest] {self.address_string()} {fmt % args}", flush=True)

    # ---- GET ----
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            return self._send(200, {"ok": True, "pool_size": len(read_lines(POOL_FILE)),
                                    "tags": all_tags(), "ts": int(time.time())})
        if not self._authed():
            return self._send(401, {"ok": False, "error": "invalid X-Api-Key"})
        if path == "/list":
            return self._send(200, {"ok": True, "proxies": read_lines(POOL_FILE)})
        return self._send(404, {"ok": False, "error": "not found"})

    # ---- POST ----
    def do_POST(self):
        path = urlparse(self.path).path
        if not self._authed():
            return self._send(401, {"ok": False, "error": "invalid X-Api-Key"})

        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length).decode("utf-8", "replace") if length else ""

        if path == "/rebuild":
            return self._send(200, {"ok": True, "pool_size": rebuild()})

        if path != "/ingest":
            return self._send(404, {"ok": False, "error": "not found"})

        tag, mode, do_validate, do_geo = "default", "append", False, True
        entries = []
        try:
            data = json.loads(raw)
            entries = data.get("proxies") or data.get("entries") or []
            tag = data.get("tag") or tag
            mode = (data.get("mode") or mode).lower()
            do_validate = bool(data.get("validate"))
            do_geo = bool(data.get("geo", True))
        except Exception:
            entries = [l.strip() for l in raw.splitlines() if l.strip()]

        entries = [e.strip() for e in entries if isinstance(e, str)]
        valid_fmt = [e for e in entries if PROXY_RE.match(e)]
        bad = len(entries) - len(valid_fmt)

        if do_validate and valid_fmt:
            valid_fmt = validate(valid_fmt)

        geo_stats = {}
        if do_geo and valid_fmt:
            found = geo_lookup(valid_fmt)
            save_geo(found)
            for cc in found.values():
                geo_stats[cc] = geo_stats.get(cc, 0) + 1

        path_t = tag_path(tag)
        existing = [] if mode == "replace" else read_lines(path_t)
        seen = set(existing)
        added = 0
        for p in valid_fmt:
            if p not in seen:
                seen.add(p)
                existing.append(p)
                added += 1

        with open(path_t, "w") as f:
            f.write("\n".join(existing) + ("\n" if existing else ""))

        size = rebuild()
        return self._send(200, {"ok": True, "tag": tag, "mode": mode,
                                "received": len(entries), "accepted": added,
                                "duplicate": len(valid_fmt) - added, "bad_format": bad,
                                "validated": do_validate, "tag_size": len(existing),
                                "pool_size": size, "countries": geo_stats})


if __name__ == "__main__":
    import threading

    rebuild()
    print(f"[ingest] pool={len(read_lines(POOL_FILE))} | ports={PORTS} | key={KEY[:6]}…", flush=True)

    servers = []
    for port in PORTS:
        try:
            srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
            servers.append(srv)
            print(f"[ingest] listening on 0.0.0.0:{port}", flush=True)
        except Exception as e:
            print(f"[ingest] GAGAL bind {port}: {e}", flush=True)

    for srv in servers[1:]:
        threading.Thread(target=srv.serve_forever, daemon=True).start()
    servers[0].serve_forever()
