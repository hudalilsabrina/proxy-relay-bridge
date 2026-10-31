#!/usr/bin/env python3
"""High-Performance Asyncio HTTP/HTTPS Forwarding Relay Bridge.

Menerima koneksi HTTP/HTTPS CONNECT dari klien (misal 9router / aplikasi AI)
pada port standar yang aman (misal 443 / 80 / 8080), lalu mem-forward dan
merotasi request secara acak (load balancing) ke ribuan upstream proxy (misal port 3129, dll).
"""
import argparse
import asyncio
import base64
import gc
import hmac
import json
import logging
import os
import random
import sys
import time
from urllib.parse import urlparse

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("proxy_relay")

proxies = []
_REQ_COUNT = 0
GC_EVERY = int(os.getenv("GC_EVERY", "10"))
UPSTREAM_TRIES = int(os.getenv("UPSTREAM_TRIES", "4"))
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "8"))
HANDSHAKE_TIMEOUT = float(os.getenv("HANDSHAKE_TIMEOUT", "10"))
# Parent proxy (opsional): semua koneksi ke upstream dilewatkan ke sini dulu.
# Dipakai kalau IP server diblokir gateway upstream (mis. FloppyData menutup :10080 untuk IP kita).
PARENT_PROXY = os.getenv("PARENT_PROXY", "").strip()
STICKY_TTL = int(os.getenv("STICKY_TTL", "900"))   # detik; sesi sticky kedaluwarsa kalau idle
# Autentikasi klien (wajib kalau bind ke 0.0.0.0).
#   RELAY_AUTH="rahasia123"       -> klien boleh pakai username apa pun + password "rahasia123"
#                                    (username dipakai untuk lane: sticky1, country-us, dst)
#   RELAY_AUTH="user:rahasia123"  -> wajib persis user:pass tersebut (lane tidak bisa dipilih)
#   kosong                        -> tanpa auth (aman HANYA untuk bind 127.0.0.1)
RELAY_AUTH = os.getenv("RELAY_AUTH", "").strip()
_sticky = {}   # key -> (index upstream, last_seen)
_countries = {}   # proxy url -> "sg"/"us"/...
_country_path = ""


def load_proxies(proxy_file: str):
    global proxies
    if not os.path.exists(proxy_file):
        logger.error(f"Proxy file not found: {proxy_file}")
        return

    with open(proxy_file, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    pool = []
    for p in lines:
        if not p.startswith("http://") and not p.startswith("https://"):
            # Check if format is ip:port:user:pass
            parts = p.split(":")
            if len(parts) == 4:
                ip, port, u, pwd = parts
                p = f"http://{u}:{pwd}@{ip}:{port}"
            elif len(parts) == 2:
                ip, port = parts
                p = f"http://{ip}:{port}"
            else:
                p = f"http://{p}"

        parsed = urlparse(p)
        if parsed.hostname and parsed.port:
            auth = None
            if parsed.username and parsed.password:
                auth = base64.b64encode(f"{parsed.username}:{parsed.password}".encode()).decode()
            pool.append({
                "host": parsed.hostname,
                "port": parsed.port,
                "auth": auth
            })

    proxies = pool

    global _countries, _country_path
    _country_path = os.path.join(os.path.dirname(os.path.abspath(proxy_file)), "countries.json")
    raw_map = {}
    try:
        with open(_country_path) as f:
            raw_map = json.load(f) or {}
    except Exception:
        raw_map = {}

    # kunci ulang: "host:port|base64(user:pass)" supaya kredensial berbeda di host
    # yang sama (mis. geo.floppydata.com:10080) tidak saling menimpa
    _countries = {}
    for url, cc in raw_map.items():
        try:
            pu = urlparse(url)
            if not pu.hostname or not pu.port:
                continue
            key = f"{pu.hostname}:{pu.port}"
            if pu.username and pu.password:
                key += "|" + base64.b64encode(f"{pu.username}:{pu.password}".encode()).decode()
            _countries[key] = (cc or "").lower()
        except Exception:
            continue

    logger.info(f"Loaded {len(proxies):,} proxies from {proxy_file} "
                f"(geo map: {len(_countries)} entri)")


async def pipe(reader, writer):
    try:
        while not reader.at_eof():
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


def client_username(header_data: bytes):
    """Ambil username dari header Proxy-Authorization klien (kalau ada)."""
    creds = client_creds(header_data)
    return creds[0] if creds else None


def client_creds(header_data: bytes):
    """Ambil (username, password) dari header Proxy-Authorization Basic klien."""
    for line in header_data.split(b"\r\n"):
        if line.lower().startswith(b"proxy-authorization:"):
            val = line.split(b":", 1)[1].strip()
            if val.lower().startswith(b"basic "):
                try:
                    dec = base64.b64decode(val[6:]).decode(errors="ignore")
                    user, _, pwd = dec.partition(":")
                    return user, pwd
                except Exception:
                    return None
    return None


def auth_ok(creds) -> bool:
    """Cek kredensial klien terhadap RELAY_AUTH."""
    if not RELAY_AUTH:
        return True                     # auth dimatikan eksplisit
    if not creds:
        return False
    user, pwd = creds
    if ":" in RELAY_AUTH:
        exp_user, _, exp_pwd = RELAY_AUTH.partition(":")
        return hmac.compare_digest(user, exp_user) and hmac.compare_digest(pwd, exp_pwd)
    return hmac.compare_digest(pwd, RELAY_AUTH)


def _country_of(entry):
    key = f"{entry['host']}:{entry['port']}"
    if entry.get("auth"):
        key += "|" + entry["auth"]
    return _countries.get(key, "")


def pick_upstream(user, tried):
    """Username bisa menentukan perilaku:
       sticky1              -> IP tetap per username
       country-sg / c-sg    -> hanya proxy dengan exit country SG
       sticky-country-sg    -> keduanya
       (kosong)             -> rotasi acak semua proxy"""
    if not proxies:
        return None

    cc = ""
    if user:
        import re as _re
        m = _re.search(r"(?:country|c)-([a-zA-Z]{2})\b", user)
        if m:
            cc = m.group(1).lower()
    strict = bool(user) and "strict" in user.lower()
    pool = [p for p in proxies if _country_of(p) == cc] if cc else proxies
    if not pool:
        if strict and cc:
            return None      # strict-country-xx: jangan diam-diam ganti negara
        pool = proxies       # negara itu belum ada di pool -> fallback semua
    if user and user.lower().startswith("sticky"):
        now = time.time()
        ent = _sticky.get(user)
        if ent and now - ent[1] < STICKY_TTL:
            cand = pool[ent[0] % len(pool)]
            if (cand["host"], cand["port"]) not in tried:
                _sticky[user] = (ent[0], now)
                return cand
            # pinned upstream sudah dicoba & gagal di request ini -> re-pin di bawah
        for _ in range(10):
            idx = random.randrange(len(pool))
            key = (pool[idx]["host"], pool[idx]["port"])
            if key not in tried or len(tried) >= len(pool):
                _sticky[user] = (idx, now)
                return pool[idx]
        return random.choice(pool)

    for _ in range(8):
        cand = random.choice(pool)
        if (cand["host"], cand["port"]) not in tried:
            return cand
    return random.choice(pool)


async def _open_via_parent(host, port):
    """Buka tunnel TCP ke host:port lewat PARENT_PROXY (HTTP CONNECT)."""
    pu = urlparse(PARENT_PROXY if "://" in PARENT_PROXY else "http://" + PARENT_PROXY)
    try:
        r, w = await asyncio.wait_for(
            asyncio.open_connection(pu.hostname, pu.port or 80),
            timeout=CONNECT_TIMEOUT
        )
    except Exception:
        return None
    req = [f"CONNECT {host}:{port} HTTP/1.1".encode(),
           f"Host: {host}:{port}".encode()]
    if pu.username:
        cred = base64.b64encode(f"{pu.username}:{pu.password or ''}".encode()).decode()
        req.append(f"Proxy-Authorization: Basic {cred}".encode())
    req += [b"Proxy-Connection: Keep-Alive", b"", b""]
    try:
        w.write(b"\r\n".join(req))
        await w.drain()
        res = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), timeout=HANDSHAKE_TIMEOUT)
        if b" 200" not in res.split(b"\r\n")[0]:
            w.close()
            return None
    except Exception:
        try:
            w.close()
        except Exception:
            pass
        return None
    return r, w


async def _try_upstream(chosen, header_data, is_connect, client_reader, client_writer, opened=None):
    """Coba satu upstream. True kalau tunnel/forward berhasil dipasang."""
    if PARENT_PROXY:
        conn = await _open_via_parent(chosen["host"], chosen["port"])
        if not conn:
            return False
        up_reader, up_writer = conn
        if opened is not None:
            opened.append(up_writer)
    else:
        try:
            up_reader, up_writer = await asyncio.wait_for(
                asyncio.open_connection(chosen["host"], chosen["port"]),
                timeout=CONNECT_TIMEOUT
            )
            if opened is not None:
                opened.append(up_writer)
        except Exception:
            return False

    lines = header_data.split(b"\r\n")

    if is_connect:
        req = [lines[0]]
        if chosen["auth"]:
            req.append(("Proxy-Authorization: Basic " + chosen["auth"]).encode())
        req.append(b"Proxy-Connection: Keep-Alive")
        req.append(b"")
        req.append(b"")
        try:
            up_writer.write(b"\r\n".join(req))
            await up_writer.drain()
            res = await asyncio.wait_for(up_reader.readuntil(b"\r\n\r\n"),
                                         timeout=HANDSHAKE_TIMEOUT)
        except Exception:
            try:
                up_writer.close()
            except Exception:
                pass
            return False
        if b" 200" not in res.split(b"\r\n")[0]:
            try:
                up_writer.close()
            except Exception:
                pass
            return False
        client_writer.write(res)
        await client_writer.drain()
    else:
        new_headers = [l for l in lines if not l.lower().startswith(b"proxy-authorization:")]
        if chosen["auth"]:
            new_headers.insert(1, ("Proxy-Authorization: Basic " + chosen["auth"]).encode())
        try:
            up_writer.write(b"\r\n".join(new_headers) + b"\r\n\r\n")
            await up_writer.drain()
        except Exception:
            try:
                up_writer.close()
            except Exception:
                pass
            return False

    await asyncio.gather(
        pipe(client_reader, up_writer),
        pipe(up_reader, client_writer)
    )
    return True


async def handle_client(client_reader, client_writer):
    try:
        header_data = await client_reader.readuntil(b"\r\n\r\n")
    except Exception:
        client_writer.close()
        return

    if not auth_ok(client_creds(header_data)):
        try:
            client_writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\n"
                                b"Proxy-Authenticate: Basic realm=\"relay\"\r\n"
                                b"Content-Length: 0\r\n\r\n")
            await client_writer.drain()
        except Exception:
            pass
        client_writer.close()
        return

    if not proxies:
        client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\nNo upstream proxies available (strict-country: negara tidak ada di pool)\r\n")
        await client_writer.drain()
        client_writer.close()
        return

    is_connect = header_data.split(b"\r\n")[0].decode(errors="ignore").startswith("CONNECT")
    user = client_username(header_data)

    tried = set()
    opened = []
    rejected = False
    for _ in range(min(UPSTREAM_TRIES, len(proxies))):
        chosen = pick_upstream(user, tried)
        if chosen is None:
            rejected = True     # strict-country & negara kosong
            break
        tried.add((chosen["host"], chosen["port"]))

        if await _try_upstream(chosen, header_data, is_connect, client_reader, client_writer, opened):
            return

    try:
        if rejected and not tried:
            client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n"
                                b"strict-country: tidak ada proxy untuk negara itu di pool\r\n")
        else:
            client_writer.write(b"HTTP/1.1 504 Gateway Timeout\r\n\r\nAll upstream attempts failed\r\n")
        await client_writer.drain()
    except Exception:
        pass
    finally:
        global _REQ_COUNT
        _REQ_COUNT += 1
        for w in opened:            # cegah kebocoran FD saat retry gagal
            try:
                w.close()
            except Exception:
                pass
        if GC_EVERY and _REQ_COUNT % GC_EVERY == 0:
            # transport dari open_connection yang dibatalkan (timeout) baru dilepas GC
            gc.collect()
        try:
            client_writer.close()
        except Exception:
            pass


async def watch_pool(proxy_file: str, interval: float = 5.0):
    """Hot reload: kalau proxies.txt berubah (ingest API), muat ulang tanpa restart."""
    try:
        last = os.path.getmtime(proxy_file)
    except Exception:
        last = 0
    while True:
        await asyncio.sleep(interval)
        try:
            m = os.path.getmtime(proxy_file)
        except Exception:
            continue
        if m != last:
            last = m
            load_proxies(proxy_file)
            logger.info(f"[hot-reload] pool sekarang {len(proxies):,} proxy")


async def main():
    parser = argparse.ArgumentParser(description="Rotating Proxy Relay Bridge")
    parser.add_argument("--host", default="0.0.0.0", help="Host address to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=443, help="Port to listen on (default: 443)")
    parser.add_argument("--proxies", default="proxies.txt", help="Path to proxy list file (default: proxies.txt)")
    args = parser.parse_args()

    load_proxies(args.proxies)
    if not proxies:
        logger.warning(f"No proxies loaded from {args.proxies}! Please make sure the file exists and is populated.")

    asyncio.create_task(watch_pool(args.proxies))
    server = await asyncio.start_server(handle_client, args.host, args.port)
    addr = server.sockets[0].getsockname()
    logger.info(f"Relay Bridge listening on {addr[0]}:{addr[1]} forwarding to {len(proxies):,} upstream proxies...")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Relay Bridge stopped by user.")
