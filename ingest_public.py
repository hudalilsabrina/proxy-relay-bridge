"""Masukkan kredensial hasil salvage ke relay (divalidasi dulu, tag terpisah)."""
import json
import urllib.request

SRC = "/opt/mrscraper-relay/bansos/bulk_alive.txt"
TAG = "public"
KEY = open("/opt/mrscraper-relay/ingest.key").read().strip()

urls = [l.strip() for l in open(SRC) if l.strip()]
print(f"kandidat: {len(urls)}", flush=True)

body = json.dumps({"proxies": urls, "tag": TAG, "mode": "append",
                   "validate": True, "geo": True}).encode()
req = urllib.request.Request("http://127.0.0.1:8080/ingest", data=body,
                             headers={"X-Api-Key": KEY, "Content-Type": "application/json"})
print(urllib.request.urlopen(req, timeout=1800).read().decode(), flush=True)
