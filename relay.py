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

    chosen = random.choice(proxies)

    try:
        upstream_reader, upstream_writer = await asyncio.wait_for(
            asyncio.open_connection(chosen["host"], chosen["port"]),
            timeout=8.0
        )
    except Exception:
        client_writer.write(b"HTTP/1.1 504 Gateway Timeout\r\n\r\nUpstream connect failed\r\n")
        await client_writer.drain()
        client_writer.close()
        return

    lines = header_data.split(b"\r\n")
    first_line = lines[0].decode(errors="ignore")

    if first_line.startswith("CONNECT"):
        # HTTPS Tunneling via CONNECT
        connect_req = [lines[0]]
        if chosen["auth"]:
            connect_req.append(f"Proxy-Authorization: Basic {chosen['auth']}".encode())
        connect_req.append(b"Proxy-Connection: Keep-Alive\r\n\r\n")

        upstream_writer.write(b"\r\n".join(connect_req))
        await upstream_writer.drain()

        try:
            upstream_res = await asyncio.wait_for(upstream_reader.readuntil(b"\r\n\r\n"), timeout=10.0)
        except Exception:
            client_writer.write(b"HTTP/1.1 504 Gateway Timeout\r\n\r\n")
            await client_writer.drain()
            client_writer.close()
            upstream_writer.close()
            return

        client_writer.write(upstream_res)
        await client_writer.drain()

        if b"200" in upstream_res.split(b"\r\n")[0]:
            await asyncio.gather(
                pipe(client_reader, upstream_writer),
                pipe(upstream_reader, client_writer)
            )
        else:
            client_writer.close()
            upstream_writer.close()

    else:
        # Standard Plain HTTP
        new_headers = []
        for line in lines:
            if line.lower().startswith(b"proxy-authorization:"):
                continue
            new_headers.append(line)

        if chosen["auth"]:
            new_headers.insert(1, f"Proxy-Authorization: Basic {chosen['auth']}".encode())

        upstream_writer.write(b"\r\n".join(new_headers) + b"\r\n\r\n")
        await upstream_writer.drain()

        await asyncio.gather(
            pipe(client_reader, upstream_writer),
            pipe(upstream_reader, client_writer)
        )


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
