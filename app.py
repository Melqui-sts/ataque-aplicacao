#!/usr/bin/env python3
"""
Servidor Alvo — Painel de Monitoramento de Resiliência (Camada de Aplicação - L7).
Projetado para atuar exclusivamente como VÍTIMA e PAINEL DE TELEMETRIA no laboratório de redes.
Todo o tráfego e estresse de CPU/RAM são recebidos externamente da Máquina B.
"""

import gc
import hashlib
import json
import os
import socket
import sys
import threading
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
import urllib.parse
import psutil

CPU_COUNT = os.cpu_count() or 2


def get_local_ip():
    """Detecta o IP da máquina na rede local para exibir no painel."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


LOCAL_IP = get_local_ip()

# Estado do Servidor Alvo
server_state = {
    "network_requests_received": 0,
    "allocated_ram_chunks": [],
    "last_request_time": 0.0,
    "start_time": time.time(),
}

state_lock = threading.Lock()


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Servidor HTTP multi-thread resiliente."""
    daemon_threads = True
    allow_reuse_address = True


HTML_DASHBOARD = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Servidor Alvo — Telemetria de Resiliência L7</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-primary: #0a0e17;
            --bg-card: rgba(18, 24, 38, 0.75);
            --bg-card-border: rgba(255, 255, 255, 0.08);
            --accent-cyan: #00f2fe;
            --accent-blue: #4facfe;
            --accent-red: #ff416c;
            --accent-emerald: #10b981;
            --accent-amber: #f59e0b;
            --text-primary: #f8fafc;
            --text-muted: #94a3b8;
            --font-main: 'Plus Jakarta Sans', sans-serif;
            --font-code: 'JetBrains Mono', monospace;
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            background: radial-gradient(circle at 50% 0%, #152238 0%, #0a0e17 80%);
            color: var(--text-primary);
            font-family: var(--font-main);
            min-height: 100vh;
            padding: 2rem;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}

        .container {{
            width: 100%;
            max-width: 1100px;
        }}

        header {{
            margin-bottom: 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--bg-card-border);
            padding-bottom: 1.5rem;
            flex-wrap: wrap;
            gap: 1rem;
        }}

        .header-title h1 {{
            font-size: 1.75rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, var(--accent-cyan), var(--accent-blue));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}

        .ip-badge {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            margin-top: 0.4rem;
            font-family: var(--font-code);
            font-size: 0.88rem;
            color: #38bdf8;
            background: rgba(56, 189, 248, 0.1);
            padding: 4px 10px;
            border-radius: 6px;
            border: 1px solid rgba(56, 189, 248, 0.25);
        }}

        .status-badge {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 16px;
            border-radius: 9999px;
            font-size: 0.85rem;
            font-weight: 700;
            background: rgba(16, 185, 129, 0.1);
            color: var(--accent-emerald);
            border: 1px solid rgba(16, 185, 129, 0.3);
            transition: all 0.3s ease;
        }}

        .status-badge.active {{
            background: rgba(255, 65, 108, 0.18);
            color: var(--accent-red);
            border-color: rgba(255, 65, 108, 0.5);
            animation: pulse 1.2s infinite;
        }}

        .status-dot {{
            width: 9px;
            height: 9px;
            border-radius: 50%;
            background-color: currentColor;
        }}

        @keyframes pulse {{
            0%, 100% {{ opacity: 1; transform: scale(1); }}
            50% {{ opacity: 0.7; transform: scale(1.2); }}
        }}

        .grid {{
            display: grid;
            grid-template-columns: repeat(12, 1fr);
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}

        .card {{
            background: var(--bg-card);
            border: 1px solid var(--bg-card-border);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(12px);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
        }}

        .metric-card {{
            grid-column: span 4;
            display: flex;
            flex-direction: column;
            gap: 1rem;
        }}

        @media (max-width: 900px) {{
            .metric-card {{ grid-column: span 12; }}
            .control-card {{ grid-column: span 12; }}
        }}

        .metric-header {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
        }}

        .metric-title {{
            color: var(--text-muted);
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: 600;
        }}

        .metric-value {{
            font-family: var(--font-code);
            font-size: 2.1rem;
            font-weight: 700;
        }}

        .progress-bar-bg {{
            background: rgba(255, 255, 255, 0.05);
            height: 10px;
            border-radius: 999px;
            overflow: hidden;
            position: relative;
        }}

        .progress-bar-fill {{
            height: 100%;
            width: 0%;
            border-radius: 999px;
            transition: width 0.3s ease, background 0.3s ease;
        }}

        .fill-cpu {{
            background: linear-gradient(90deg, #00f2fe, #ff416c);
        }}

        .fill-ram {{
            background: linear-gradient(90deg, #4facfe, #9b51e0);
        }}

        .fill-swap {{
            background: linear-gradient(90deg, #f59e0b, #ef4444);
        }}

        .metric-sub {{
            font-size: 0.8rem;
            color: var(--text-muted);
            font-family: var(--font-code);
        }}

        .info-card {{
            grid-column: span 12;
            background: linear-gradient(90deg, rgba(56, 189, 248, 0.08) 0%, rgba(99, 102, 241, 0.08) 100%);
            border: 1px solid rgba(56, 189, 248, 0.25);
            border-radius: 14px;
            padding: 1.25rem 1.5rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }}

        .info-text h3 {{
            font-size: 1rem;
            font-weight: 700;
            color: #38bdf8;
            margin-bottom: 0.25rem;
        }}

        .info-text p {{
            font-size: 0.85rem;
            color: #cbd5e1;
            line-height: 1.5;
        }}

        .btn-reset {{
            background: rgba(255, 255, 255, 0.08);
            color: var(--text-primary);
            border: 1px solid var(--bg-card-border);
            padding: 10px 18px;
            border-radius: 8px;
            font-size: 0.88rem;
            font-weight: 600;
            font-family: var(--font-main);
            cursor: pointer;
            transition: all 0.2s ease;
        }}

        .btn-reset:hover {{
            background: rgba(255, 255, 255, 0.16);
            border-color: rgba(255, 255, 255, 0.4);
        }}

        .logs-card {{
            grid-column: span 12;
            background: rgba(5, 8, 15, 0.95);
            border-radius: 12px;
            padding: 1.1rem 1.4rem;
            border: 1px solid var(--bg-card-border);
            font-family: var(--font-code);
            font-size: 0.85rem;
            color: #a5b4fc;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-title">
                <h1>Painel de Telemetria — Servidor Alvo (L7)</h1>
                <div class="ip-badge">
                    📡 Escutando na Rede: <strong>http://{LOCAL_IP}:8080</strong>
                </div>
            </div>
            <div id="statusBadge" class="status-badge">
                <span class="status-dot"></span>
                <span id="statusText">Aguardando Tráfego da Rede</span>
            </div>
        </header>

        <div class="grid">
            <!-- Métrica de CPU -->
            <div class="card metric-card">
                <div class="metric-header">
                    <span class="metric-title">Uso de CPU</span>
                    <span id="cpuValue" class="metric-value">0%</span>
                </div>
                <div class="progress-bar-bg">
                    <div id="cpuBar" class="progress-bar-fill fill-cpu"></div>
                </div>
                <div class="metric-sub" id="cpuCores">{CPU_COUNT} núcleos detectados</div>
            </div>

            <!-- Métrica de RAM -->
            <div class="card metric-card">
                <div class="metric-header">
                    <span class="metric-title">Memória RAM</span>
                    <span id="ramValue" class="metric-value">0%</span>
                </div>
                <div class="progress-bar-bg">
                    <div id="ramBar" class="progress-bar-fill fill-ram"></div>
                </div>
                <div class="metric-sub" id="ramDetails">0 GB / 0 GB alocados</div>
            </div>

            <!-- Métrica de SWAP -->
            <div class="card metric-card">
                <div class="metric-header">
                    <span class="metric-title">Memória SWAP (Disco)</span>
                    <span id="swapValue" class="metric-value">0%</span>
                </div>
                <div class="progress-bar-bg">
                    <div id="swapBar" class="progress-bar-fill fill-swap"></div>
                </div>
                <div class="metric-sub" id="swapDetails">0 GB / 0 GB usados</div>
            </div>

            <!-- Informações de Arquitetura e Reset -->
            <div class="info-card">
                <div class="info-text">
                    <h3>🏛️ Modo de Operação: Alvo Passivo na Rede</h3>
                    <p>
                        Este servidor está recebendo e processando tráfego HTTP autêntico no endpoint <code>POST /api/workload</code>.<br>
                        Para disparar o teste, execute o <code>client_flood.py</code> na <strong>Máquina B (Atacante)</strong> apontando para <strong>http://{LOCAL_IP}:8080</strong>.
                    </p>
                </div>
                <button class="btn-reset" onclick="resetMemory()">
                    🧹 Limpar Memória / Resetar Contadores
                </button>
            </div>

            <!-- Telemetria em Tempo Real -->
            <div class="logs-card">
                <span>Requisições L7 Recebidas: <strong id="netReqCount" style="color: #38bdf8;">0</strong></span>
                <span>Memória Retida no Heap: <strong id="appCacheMb" style="color: #fff;">0 MB</strong></span>
                <span>Tempo Ativo: <strong id="uptimeVal" style="color: #fff;">0s</strong></span>
            </div>
        </div>
    </div>

    <script>
        async function fetchStats() {{
            try {{
                const res = await fetch('/api/stats');
                const data = await res.json();

                // Atualiza CPU
                document.getElementById('cpuValue').innerText = data.cpu_percent.toFixed(1) + '%';
                document.getElementById('cpuBar').style.width = Math.min(100, data.cpu_percent) + '%';
                document.getElementById('cpuCores').innerText = `${{data.cpu_count}} núcleos detectados`;

                // Atualiza RAM
                document.getElementById('ramValue').innerText = data.ram_percent.toFixed(1) + '%';
                document.getElementById('ramBar').style.width = Math.min(100, data.ram_percent) + '%';
                document.getElementById('ramDetails').innerText = `${{data.ram_used_gb.toFixed(2)}} GB de ${{data.ram_total_gb.toFixed(2)}} GB (${{data.ram_percent.toFixed(0)}}%)`;

                // Atualiza SWAP
                document.getElementById('swapValue').innerText = data.swap_percent.toFixed(1) + '%';
                document.getElementById('swapBar').style.width = Math.min(100, data.swap_percent) + '%';
                document.getElementById('swapDetails').innerText = `${{data.swap_used_gb.toFixed(2)}} GB de ${{data.swap_total_gb.toFixed(2)}} GB (${{data.swap_percent.toFixed(0)}}%)`;

                // Telemetria de Rede
                document.getElementById('netReqCount').innerText = (data.network_requests_received || 0).toLocaleString();
                document.getElementById('appCacheMb').innerText = data.app_cache_mb + ' MB';
                document.getElementById('uptimeVal').innerText = data.uptime_sec + 's';

                // Atualiza badge de tráfego ativo
                const badge = document.getElementById('statusBadge');
                const badgeText = document.getElementById('statusText');
                if (data.is_under_attack) {{
                    badge.className = 'status-badge active';
                    badgeText.innerText = '🚨 Sob Tráfego L7 Ativo';
                }} else {{
                    badge.className = 'status-badge';
                    badgeText.innerText = 'Aguardando Tráfego da Rede';
                }}
            }} catch (err) {{
                console.error("Falha ao obter status:", err);
            }}
        }}

        async function resetMemory() {{
            await fetch('/api/reset', {{ method: 'POST' }});
            fetchStats();
        }}

        setInterval(fetchStats, 1000);
        fetchStats();
    </script>
</body>
</html>
"""


class AppRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Desativa logs no terminal para economizar I/O durante o teste
        return

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path in ("/", "/index.html"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_DASHBOARD.encode("utf-8"))
            return

        elif parsed.path == "/api/stats":
            cpu_pct = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            swap = psutil.swap_memory()

            now = time.time()
            with state_lock:
                cache_size = sum(len(c) for c in server_state["allocated_ram_chunks"])
                is_under_attack = (now - server_state["last_request_time"]) < 2.5
                uptime = int(now - server_state["start_time"])

                payload = {
                    "cpu_percent": cpu_pct,
                    "cpu_count": CPU_COUNT,
                    "ram_percent": mem.percent,
                    "ram_used_gb": (mem.total - mem.available) / (1024**3),
                    "ram_total_gb": mem.total / (1024**3),
                    "swap_percent": swap.percent,
                    "swap_used_gb": swap.used / (1024**3),
                    "swap_total_gb": swap.total / (1024**3),
                    "network_requests_received": server_state["network_requests_received"],
                    "app_cache_mb": cache_size // (1024 * 1024),
                    "is_under_attack": is_under_attack,
                    "uptime_sec": uptime,
                }

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode("utf-8"))
            return

        self.send_error(404, "Endpoint não encontrado")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"

        try:
            data = json.loads(body.decode("utf-8")) if body else {}
        except json.JSONDecodeError:
            data = {}

        # Endpoint autêntico de Camada 7 para receber tráfego da Máquina Atacante
        if parsed.path == "/api/workload":
            mode = data.get("mode", "both")
            rounds = int(data.get("rounds", 15000))
            chunk_mb = int(data.get("chunk_mb", 10))

            start_t = time.time()

            # 1. Carga de CPU sob demanda por requisição HTTP recebida da rede
            if mode in ("cpu", "both"):
                h = b"academic_l7_workload_payload" * 32
                for _ in range(rounds):
                    h = hashlib.sha256(h).digest()

            # 2. Carga de RAM sob demanda por requisição HTTP recebida da rede
            if mode in ("ram", "both"):
                try:
                    chunk = bytearray(b"\xff" * (chunk_mb * 1024 * 1024))
                    with state_lock:
                        server_state["allocated_ram_chunks"].append(chunk)
                except MemoryError:
                    pass

            now = time.time()
            with state_lock:
                server_state["network_requests_received"] += 1
                server_state["last_request_time"] = now

            elapsed_ms = (now - start_t) * 1000

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "processed",
                "latency_ms": round(elapsed_ms, 2)
            }).encode("utf-8"))
            return

        elif parsed.path in ("/api/reset", "/api/simulation/reset"):
            with state_lock:
                server_state["allocated_ram_chunks"].clear()
                server_state["network_requests_received"] = 0
                gc.collect()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "reset"}).encode("utf-8"))
            return

        self.send_error(404, "Endpoint não encontrado")


def run_server(port=8080):
    psutil.cpu_percent(interval=None)

    server_address = ("0.0.0.0", port)
    httpd = ThreadedHTTPServer(server_address, AppRequestHandler)
    print("=" * 65)
    print("  SERVIDOR ALVO — MONITORAMENTO DE RESILIÊNCIA (L7)")
    print("=" * 65)
    print(f"[*] Núcleos de CPU detectados: {CPU_COUNT}")
    print(f"[*] IP na Rede Local:          http://{LOCAL_IP}:{port}")
    print(f"[*] Endpoint de Carga L7:      http://{LOCAL_IP}:{port}/api/workload")
    print("=" * 65)
    print("[*] Aguardando tráfego do client_flood.py da Máquina B...")
    print("    Pressione [Ctrl + C] no terminal para encerrar.")
    print("=" * 65)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[!] Encerrando servidor alvo...")
        httpd.shutdown()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    run_server(port)
