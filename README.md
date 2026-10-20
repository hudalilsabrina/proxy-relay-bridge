# 🚀 Proxy Relay Bridge (Rotating Port 443 / 80 Forwarder)

Lightweight, high-concurrency Python asyncio forward proxy bridge.

Designed to solve **firewall outbound port restrictions** (e.g. servers blocking all ports except `80` and `443`) and provide **automatic random rotation / load-balancing** across a pool of thousands of upstream authenticated proxies (e.g. ProxyScrape, Webshare, BrightData, residential proxies on ports like `3129`, `8080`, `10000+`).

---

## ⚡ Features
- **Firewall Bypass**: Listens on standard ports (`443`, `80`, or custom) that are never blocked by cloud firewalls.
- **Auto Rotation**: Every incoming HTTP/HTTPS request is transparently load-balanced across your proxy pool.
- **Full HTTPS CONNECT Tunneling**: Bi-directional streaming for SSL/TLS connections without decrypting or tampering with user traffic.
- **Multi-Format Upstream Support**:
  - `http://user:password@ip:port`
  - `ip:port:user:password`
  - `ip:port`
- **Zero Heavy Dependencies**: Built entirely with Python's standard library (`asyncio`, `urllib`). No external pip dependencies required.
- **Ideal for AI Gateways**: Seamlessly pairs with gateways like **9router**, **LiteLLM**, **One-API**, or custom scrapers.

---

## 🛠️ Quick Start

### 1. Clone & Setup
```bash
git clone https://github.com/hudalilsabrina/proxy-relay-bridge.git
cd proxy-relay-bridge
```

### 2. Prepare Your Proxy List
Create a `proxies.txt` file and paste your proxies:
```text
http://user1:pass1@104.207.33.129:3129
http://user2:pass2@104.207.33.171:3129
# or
104.207.33.129:3129:user1:pass1
```

### 3. Run the Relay Bridge
```bash
python3 relay.py --port 443 --proxies proxies.txt
```
*(Note: Binding to port 443 or 80 requires root privileges on Linux).*

---

## 🧪 Testing Your Relay

Test directly with `curl`:
```bash
# Request 1 -> Egress IP A
curl -x "http://127.0.0.1:443" https://api.ipify.org

# Request 2 -> Egress IP B (Rotates automatically!)
curl -x "http://127.0.0.1:443" https://api.ipify.org
```

---

## ⚙️ Running as a Systemd Service (Linux)

1. Copy repository to `/opt/proxy-relay-bridge`:
   ```bash
   sudo cp -r . /opt/proxy-relay-bridge
   ```
2. Copy and enable the systemd service:
   ```bash
   sudo cp proxy-relay.service.example /etc/systemd/system/proxy-relay.service
   sudo systemctl daemon-reload
   sudo systemctl enable --now proxy-relay
   ```
3. Check status:
   ```bash
   sudo systemctl status proxy-relay
   ```

---

## 📄 License
MIT License. Open source and free for public use.
