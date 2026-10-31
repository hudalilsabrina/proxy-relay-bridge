# 🚀 Proxy Relay Bridge

Lightweight asyncio forward-proxy bridge with **rotation, sticky sessions, country routing, an
ingest API and self-pruning**. Stdlib only — no pip dependencies.

Built for the case where the *client* can only reach standard web ports (`80`/`443`) while the
upstream proxies live on non-standard ports (`3129`, `8080`, `10000`, `10080`, …), and the pool
keeps churning because trial proxies die every few hours.

---

## ⚡ Features

- **Firewall bypass** — listen on `80`/`443`, forward to any upstream port.
- **Rotating mode** — a different upstream per request.
- **Sticky mode** — one upstream pinned per username (`sticky1`, `sticky2`, …) with TTL.
- **Country routing** — `country-sg`, `country-us`, … picks only upstreams whose exit country matches.
- **Per-request retry** — up to `UPSTREAM_TRIES` different upstreams before returning `504`.
- **Hot reload** — the pool file is watched; edits apply within seconds, no restart.
- **Ingest API** — POST proxy lists from any agent/box; JSON or plain text, optional validation.
- **Auto-prune** — systemd timer re-tests the pool and drops dead proxies.
- **Multi-format upstreams** — `http://user:pass@ip:port`, `ip:port:user:pass`, `ip:port`.

---

## 🛠️ Quick Start

```bash
git clone https://github.com/hudalilsabrina/proxy-relay-bridge.git
cd proxy-relay-bridge
cp proxies.example.txt proxies.txt      # isi proxy kamu
python3 relay.py --port 443 --proxies proxies.txt
```

Binding `80`/`443` requires root.

### Modes (pilih lewat username proxy)

| URL | Perilaku |
|---|---|
| `http://relay-host:80` | rotasi acak semua upstream |
| `http://sticky1:x@relay-host:80` | IP tetap untuk lane `sticky1` (TTL `STICKY_TTL`, default 900 s) |
| `http://country-sg:x@relay-host:80` | hanya upstream dengan exit country SG |
| `http://strict-country-sg:x@relay-host:80` | sama, tapi **502** kalau negara itu tidak ada di pool (tidak fallback) |
| `http://sticky-country-de:x@relay-host:80` | sticky + filter negara |

Kalau negara yang diminta tidak ada di pool, mode `country-xx` fallback ke semua upstream
(tidak error), sedangkan `strict-country-xx` menolak dengan `502` — pakai yang strict kalau
hasil scraping tidak boleh keluar dari negara target.

> Catatan: `countries.json` **harus** ber-key URL proxy (`http://user:pass@host:port`),
> karena `relay.py` mem-parse setiap key sebagai URL. Key `host:port|base64` akan membuat
> peta terbaca 0 entri dan filter negara diam-diam mati. `prune.py` sudah otomatis
> me-refresh peta ini setiap kali prune jalan.

---

## 🔐 Client Auth (wajib kalau bind publik)

`relay.py` tidak punya auth bawaan, jadi **kalau `RELAY_HOST=0.0.0.0` kamu HARUS set `RELAY_AUTH`**:

```bash
# /etc/default/proxy-relay
RELAY_HOST=0.0.0.0
RELAY_PORT=8443
RELAY_AUTH=rahasia-kuat-disini     # password; username bebas (dipakai utk lane)
```

| `RELAY_AUTH` | Perilaku |
|---|---|
| *(kosong)* | Tanpa auth — aman HANYA untuk bind `127.0.0.1` |
| `password` | Klien wajib kirim `Proxy-Authorization: Basic <bebas>:password`; username tetap bebas untuk lane (`sticky1`, `country-us`, …) |
| `user:password` | Wajib persis `user:password` tersebut; lane tidak bisa dipilih |

Kredensial dicek dengan `hmac.compare_digest` (timing-safe). Tanpa/salah password → `407 Proxy Authentication Required`.

```bash
curl -x http://anyuser:rahasia-kuat-disini@relay-host:8443 https://api.ipify.org
```

---

## 📥 Ingest API

`ingest.py` menerima proxy dari mesin/agent lain dan menulisnya ke `pool.d/<tag>.txt`,
lalu relay hot-reload sendiri.

```bash
# JSON
curl -X POST http://relay-host:8080/ingest \
  -H "X-Api-Key: $INGEST_KEY" -H "Content-Type: application/json" \
  -d '{"proxies":["http://user:pass@1.2.3.4:8080"],"tag":"agent1","mode":"append","validate":true}'

# text/plain, satu proxy per baris
curl -X POST http://relay-host:8080/ingest -H "X-Api-Key: $INGEST_KEY" --data-binary @list.txt

curl http://relay-host:8080/health          # tanpa auth
curl http://relay-host:8080/list -H "X-Api-Key: $INGEST_KEY"
```

| Field | Arti |
|---|---|
| `tag` | nama sumber → `pool.d/<tag>.txt` (bisa dicabut per sumber) |
| `mode` | `append` (default) / `replace` |
| `validate` | tes dulu ke `api.ipify.org`, hanya yang hidup disimpan |
| `geo` | cari exit country tiap proxy → `countries.json` (dipakai filter `country-xx`) |

Key dibuat otomatis di `ingest.key` saat pertama jalan (chmod 600, jangan di-commit).
Default listen di **dua port**: `8787` dan `8080` (`INGEST_PORTS`), karena banyak host
memblokir egress ke port custom.

---

## 🧹 Auto-prune

`prune.py` mengetes semua isi `pool.d/*.txt`, membuang yang gagal, lalu rebuild `proxies.txt`.

```bash
systemctl enable --now relay-prune.timer    # tiap 10 menit
systemctl start relay-prune.service         # prune manual
```

---

## ⚙️ Systemd

Unit siap pakai ada di `deploy/`:

| File | Fungsi |
|---|---|
| `deploy/relay.service` | relay utama (contoh port 443) |
| `deploy/relay-ingest.service` | ingest API (`8787,8080`) |
| `deploy/relay-prune.service` + `.timer` | prune periodik |

```bash
sudo cp deploy/*.service deploy/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now relay relay-ingest relay-prune.timer
```

---

## 🧪 Testing

```bash
# rotasi: IP berubah tiap request
curl -x http://127.0.0.1:443 https://api.ipify.org

# sticky: IP sama terus
for i in 1 2 3; do curl -x http://sticky1:x@127.0.0.1:443 https://api.ipify.org; done

# country routing
curl -x http://country-sg:x@127.0.0.1:80 https://ipinfo.io/json
```

---

## 📄 License

MIT.
