#!/usr/bin/env python3
"""High-Performance Asyncio HTTP/HTTPS Forwarding Relay Bridge.

Menerima koneksi HTTP/HTTPS CONNECT dari klien (misal 9router / aplikasi AI)
pada port standar yang aman (misal 443 / 80 / 8080), lalu mem-forward dan
merotasi request secara acak (load balancing) ke ribuan upstream proxy (misal port 3129, dll).
"""
import argparse
import asyncio
import base64
import logging
import os
import random
import sys
from urllib.parse import urlparse

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("proxy_relay")

proxies = []
UPSTREAM_TRIES = int(os.getenv("UPSTREAM_TRIES", "4"))
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "8"))
HANDSHAKE_TIMEOUT = float(os.getenv("HANDSHAKE_TIMEOUT", "10"))


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
    logger.info(f"Loaded {len(proxies):,} proxies from {proxy_file}")


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


async def _try_upstream(chosen, header_data, is_connect, client_reader, client_writer):
    """Coba satu upstream. True kalau tunnel/forward berhasil dipasang."""
    try:
        up_reader, up_writer = await asyncio.wait_for(
            asyncio.open_connection(chosen["host"], chosen["port"]),
            timeout=CONNECT_TIMEOUT
        )
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

    if not proxies:
        client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\nNo upstream proxies available\r\n")
        await client_writer.drain()
        client_writer.close()
        return

    is_connect = header_data.split(b"\r\n")[0].decode(errors="ignore").startswith("CONNECT")

    tried = set()
    for _ in range(min(UPSTREAM_TRIES, len(proxies))):
        chosen = None
        for _ in range(6):
            cand = random.choice(proxies)
            key = (cand["host"], cand["port"])
            if key not in tried:
                chosen = cand
                break
        if chosen is None:
            chosen = random.choice(proxies)
        tried.add((chosen["host"], chosen["port"]))

        if await _try_upstream(chosen, header_data, is_connect, client_reader, client_writer):
            return

    try:
        client_writer.write(b"HTTP/1.1 504 Gateway Timeout\r\n\r\nAll upstream attempts failed\r\n")
        await client_writer.drain()
    except Exception:
        pass
    finally:
        try:
            client_writer.close()
        except Exception:
            pass


async def main():
    parser = argparse.ArgumentParser(description="Rotating Proxy Relay Bridge")
    parser.add_argument("--host", default="0.0.0.0", help="Host address to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=443, help="Port to listen on (default: 443)")
    parser.add_argument("--proxies", default="proxies.txt", help="Path to proxy list file (default: proxies.txt)")
    args = parser.parse_args()

    load_proxies(args.proxies)
    if not proxies:
        logger.warning(f"No proxies loaded from {args.proxies}! Please make sure the file exists and is populated.")

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
