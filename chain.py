"""Chained proxy checker.

Kalau PARENT_PROXY diset, semua koneksi ke gateway upstream dilewatkan ke parent
(HTTP CONNECT) dulu — dipakai saat IP server diblokir gateway upstream, mis.
FloppyData menutup :10080 untuk IP kita.

Dipakai oleh prune.py dan ingest.py supaya pengecekan proxy tetap akurat.
"""
import base64
import os
import socket
import time
from urllib.parse import urlparse

PARENT_PROXY = os.getenv("PARENT_PROXY", "").strip()
DEFAULT_TIMEOUT = float(os.getenv("CHAIN_TIMEOUT", "15"))


def _parent_addr():
    pu = urlparse(PARENT_PROXY if "://" in PARENT_PROXY else "http://" + PARENT_PROXY)
    return pu.hostname, (pu.port or 80), pu.username, pu.password


def _parent_connect(host, port, timeout):
    ph, pp, pu_user, pu_pass = _parent_addr()
    s = socket.create_connection((ph, pp), timeout=timeout)
    req = [f"CONNECT {host}:{port} HTTP/1.1".encode(),
           f"Host: {host}:{port}".encode()]
    if pu_user:
        cred = base64.b64encode(f"{pu_user}:{pu_pass or ''}".encode()).decode()
        req.append(f"Proxy-Authorization: Basic {cred}".encode())
    req += [b"Proxy-Connection: Keep-Alive", b"", b""]
    s.sendall(b"\r\n".join(req))

    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = s.recv(4096)
        if not chunk:
            s.close()
            raise OSError("parent menutup koneksi saat CONNECT")
        buf += chunk
    head = buf.split(b"\r\n\r\n")[0].split(b"\r\n")[0]
    if b" 200" not in head:
        s.close()
        raise OSError(f"parent CONNECT gagal: {head.decode(errors='ignore')[:60]}")
    return s


def open_tunnel(host, port, timeout=DEFAULT_TIMEOUT, attempts=4):
    """Buka socket ke host:port.

    Tanpa PARENT_PROXY: koneksi langsung.
    Dengan PARENT_PROXY: CONNECT lewat parent, dicoba beberapa kali karena parent
    yang berputar (relay datacenter) sebagian menolak port non-standar seperti 10080.
    Kalau semua percobaan via parent gagal, coba koneksi langsung sebagai fallback.
    """
    if not PARENT_PROXY:
        return socket.create_connection((host, port), timeout=timeout)

    last = None
    for i in range(attempts):
        try:
            return _parent_connect(host, port, timeout)
        except Exception as e:
            last = e
            if i < attempts - 1:
                time.sleep(0.25)

    try:
        return socket.create_connection((host, port), timeout=timeout)
    except Exception:
        raise last if last else OSError("gagal membuka tunnel")


def _recv_all(s, limit=8192):
    data = b""
    try:
        while len(data) < limit:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
            if b"\r\n\r\n" in data and len(data) > 64:
                # sudah dapat header + sebagian body
                body = data.split(b"\r\n\r\n", 1)[1]
                if body.strip():
                    break
    except Exception:
        pass
    return data


def geo(proxy_url, timeout=DEFAULT_TIMEOUT):
    """Negara keluar proxy lewat ip-api.com (HTTP, jadi bisa di-chain).
    Return (country_code_lower, exit_ip) atau ('', None)."""
    ok, _ip, _err = (False, None, None)
    try:
        u = urlparse(proxy_url if "://" in proxy_url else "http://" + proxy_url)
        host, port = u.hostname, u.port
        if not host or not port:
            return "", None
    except Exception:
        return "", None

    target = "http://ip-api.com/json/?fields=status,countryCode,query"
    sock = None
    try:
        sock = open_tunnel(host, port, timeout=timeout)
        lines = [f"GET {target} HTTP/1.1".encode(),
                 b"Host: ip-api.com",
                 b"User-Agent: curl/8.0",
                 b"Connection: close"]
        if u.username:
            cred = base64.b64encode(f"{u.username}:{u.password or ''}".encode()).decode()
            lines.append(f"Proxy-Authorization: Basic {cred}".encode())
        lines += [b"", b""]
        sock.sendall(b"\r\n".join(lines))
        raw = _recv_all(sock)
        body = raw.split(b"\r\n\r\n", 1)[1].decode(errors="ignore").strip()
        import json as _json
        d = _json.loads(body)
        if d.get("status") == "success":
            return (d.get("countryCode") or "").lower(), d.get("query")
    except Exception:
        pass
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass
    return "", None


def check(proxy_url, target="http://api.ipify.org/", timeout=DEFAULT_TIMEOUT):
    """Tes satu proxy HTTP: return (alive: bool, exit_ip: str|None, err: str|None)."""
    try:
        u = urlparse(proxy_url if "://" in proxy_url else "http://" + proxy_url)
        host, port = u.hostname, u.port
        if not host or not port:
            return False, None, "format proxy tidak valid"
    except Exception as e:
        return False, None, f"parse: {e}"

    tu = urlparse(target)
    path = tu.path or "/"
    sock = None
    try:
        sock = open_tunnel(host, port, timeout=timeout)
        lines = [f"GET {target} HTTP/1.1".encode(),
                 f"Host: {tu.hostname}".encode(),
                 b"User-Agent: curl/8.0",
                 b"Connection: close"]
        if u.username:
            cred = base64.b64encode(f"{u.username}:{u.password or ''}".encode()).decode()
            lines.append(f"Proxy-Authorization: Basic {cred}".encode())
        lines += [b"", b""]
        sock.sendall(b"\r\n".join(lines))
        raw = _recv_all(sock)
        head = raw.split(b"\r\n\r\n")[0].decode(errors="ignore")
        status = head.split(" ")[1] if " " in head else ""
        if status != "200":
            return False, None, f"HTTP {status or '?'}"
        body = raw.split(b"\r\n\r\n", 1)[1].decode(errors="ignore").strip()
        return (True, body[:40] or None, None) if body else (False, None, "body kosong")
    except Exception as e:
        return False, None, str(e)[:80]
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass
