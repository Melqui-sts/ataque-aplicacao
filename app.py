#!/usr/bin/env python3
"""
Laboratório de Teste de Carga e Resiliência na Camada de Aplicação (L7).
Otimizado para atingir 100% real de CPU (via multiprocessamento) e exaustão física de RAM.
"""

import gc
import json
import multiprocessing
import os
import sys
import threading
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
import urllib.parse
import psutil

# Estado global da aplicação
system_state = {
    "simulation_running": False,
    "simulation_mode": "both",  # 'cpu', 'ram', 'both'
    "concurrency": 8,
    "requests_processed": 0,
    "allocated_ram_chunks": [],
    "cpu_processes": [],
    "stop_event": multiprocessing.Event(),
}

state_lock = threading.Lock()


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Servidor HTTP multi-thread para lidar com alto volume de conexões."""
    daemon_threads = True
    allow_reuse_address = True


HTML_DASHBOARD = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Laboratório de Carga e Resiliência (Camada de Aplicação - L7)</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0e17;
            --bg-card: rgba(18, 24, 38, 0.75);
            --bg-card-border: rgba(255, 255, 255, 0.08);
            --accent-cyan: #00f2fe;
            --accent-blue: #4facfe;
            --accent-red: #ff416c;
            --accent-orange: #ff4b2b;
            --accent-emerald: #10b981;
            --accent-amber: #f59e0b;
            --text-primary: #f8fafc;
            --text-muted: #94a3b8;
            --font-main: 'Plus Jakarta Sans', sans-serif;
            --font-code: 'JetBrains Mono', monospace;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            background: radial-gradient(circle at 50% 0%, #152238 0%, #0a0e17 80%);
            color: var(--text-primary);
            font-family: var(--font-main);
            min-height: 100vh;
            padding: 2rem;
            display: flex;
            flex-direction: column;
            align-items: center;
        }

        .container {
            width: 100%;
            max-width: 1100px;
        }

        header {
            margin-bottom: 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--bg-card-border);
            padding-bottom: 1.5rem;
        }

        .header-title h1 {
            font-size: 1.75rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, var(--accent-cyan), var(--accent-blue));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 6px 14px;
            border-radius: 9999px;
            font-size: 0.85rem;
            font-weight: 600;
            background: rgba(16, 185, 129, 0.1);
            color: var(--accent-emerald);
            border: 1px solid rgba(16, 185, 129, 0.3);
            transition: all 0.3s ease;
        }

        .status-badge.active {
            background: rgba(255, 65, 108, 0.15);
            color: var(--accent-red);
            border-color: rgba(255, 65, 108, 0.4);
            animation: pulse 1.5s infinite;
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background-color: currentColor;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.7; transform: scale(1.15); }
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(12, 1fr);
            gap: 1.5rem;
            margin-bottom: 2rem;
        }

        .card {
            background: var(--bg-card);
            border: 1px solid var(--bg-card-border);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(12px);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
        }

        .metric-card {
            grid-column: span 4;
            display: flex;
            flex-direction: column;
            gap: 1rem;
        }

        @media (max-width: 900px) {
            .metric-card { grid-column: span 12; }
            .control-card { grid-column: span 12; }
        }

        .metric-header {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
        }

        .metric-title {
            color: var(--text-muted);
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: 600;
        }

        .metric-value {
            font-family: var(--font-code);
            font-size: 2rem;
            font-weight: 700;
        }

        .progress-bar-bg {
            background: rgba(255, 255, 255, 0.05);
            height: 10px;
            border-radius: 999px;
            overflow: hidden;
            position: relative;
        }

        .progress-bar-fill {
            height: 100%;
            width: 0%;
            border-radius: 999px;
            transition: width 0.3s ease, background 0.3s ease;
        }

        .fill-cpu {
            background: linear-gradient(90deg, #00f2fe, #ff416c);
        }

        .fill-ram {
            background: linear-gradient(90deg, #4facfe, #9b51e0);
        }

        .fill-swap {
            background: linear-gradient(90deg, #f59e0b, #ef4444);
        }

        .metric-sub {
            font-size: 0.8rem;
            color: var(--text-muted);
            font-family: var(--font-code);
        }

        .alert-box {
            grid-column: span 12;
            background: linear-gradient(90deg, rgba(245, 158, 11, 0.12) 0%, rgba(239, 68, 68, 0.08) 100%);
            border: 1px solid rgba(245, 158, 11, 0.35);
            border-left: 4px solid #f59e0b;
            border-radius: 12px;
            padding: 1.1rem 1.4rem;
            display: flex;
            gap: 1rem;
            align-items: flex-start;
        }

        .alert-icon {
            font-size: 1.5rem;
            line-height: 1;
            flex-shrink: 0;
        }

        .alert-content h3 {
            font-size: 0.95rem;
            font-weight: 700;
            color: #fbbf24;
            margin-bottom: 0.3rem;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .alert-content p {
            font-size: 0.85rem;
            color: #cbd5e1;
            line-height: 1.5;
        }

        .alert-tag {
            background: rgba(245, 158, 11, 0.2);
            color: #fbbf24;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-family: var(--font-code);
        }

        .control-card {
            grid-column: span 12;
            display: flex;
            flex-direction: column;
            gap: 1.5rem;
        }

        .form-row {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1.5rem;
        }

        .form-group {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }

        label {
            font-size: 0.85rem;
            font-weight: 600;
            color: var(--text-muted);
        }

        select, input[type="range"] {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--bg-card-border);
            color: var(--text-primary);
            padding: 10px 14px;
            border-radius: 8px;
            font-size: 0.95rem;
            font-family: var(--font-main);
            outline: none;
        }

        select option {
            background: #111827;
            color: #fff;
        }

        .slider-container {
            display: flex;
            align-items: center;
            gap: 1rem;
        }

        .slider-val {
            font-family: var(--font-code);
            font-weight: bold;
            min-width: 35px;
            color: var(--accent-cyan);
        }

        .actions-row {
            display: flex;
            gap: 1rem;
            align-items: center;
            flex-wrap: wrap;
        }

        button {
            border: none;
            padding: 12px 24px;
            border-radius: 10px;
            font-size: 0.95rem;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.2s ease;
            display: inline-flex;
            align-items: center;
            gap: 8px;
            font-family: var(--font-main);
        }

        .btn-start {
            background: linear-gradient(135deg, #ff416c, #ff4b2b);
            color: #fff;
            box-shadow: 0 4px 15px rgba(255, 75, 43, 0.4);
        }

        .btn-start:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(255, 75, 43, 0.6);
        }

        .btn-stop {
            background: rgba(255, 255, 255, 0.1);
            color: var(--text-primary);
            border: 1px solid var(--bg-card-border);
        }

        .btn-stop:hover {
            background: rgba(255, 255, 255, 0.18);
        }

        .btn-reset {
            background: transparent;
            color: var(--text-muted);
            border: 1px dashed var(--bg-card-border);
        }

        .btn-reset:hover {
            color: var(--text-primary);
            border-color: rgba(255, 255, 255, 0.4);
        }

        .logs-card {
            grid-column: span 12;
            background: rgba(5, 8, 15, 0.9);
            border-radius: 12px;
            padding: 1rem 1.25rem;
            border: 1px solid var(--bg-card-border);
            font-family: var(--font-code);
            font-size: 0.82rem;
            color: #a5b4fc;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-title">
                <h1>Painel de Resiliência e Carga (Camada de Aplicação)</h1>
            </div>
            <div id="statusBadge" class="status-badge">
                <span class="status-dot"></span>
                <span id="statusText">Sistema Ocioso</span>
            </div>
        </header>

        <div class="grid">
            <!-- Alerta de Simulação de Alta Carga (HTTP Flood L7) -->
            <div class="alert-box">
                <div class="alert-icon">⚠️</div>
                <div class="alert-content">
                    <h3>AVISO DE ALTA CARGA <span class="alert-tag">SIMULAÇÃO HTTP FLOOD (L7)</span></h3>
                    <p>
                        Este experimento dispara um volume massivo de requisições sobre os endpoints da aplicação. 
                        <strong>Diferente de ataques de rede volumétricos (como SYN Flood)</strong>, o <strong>HTTP Flood na Camada de Aplicação (L7)</strong> consome CPU e força o commit físico de páginas de memória RAM, podendo provocar lentidão extrema ou travamento na Máquina Virtual. Execute exclusivamente em ambiente de testes.
                    </p>
                </div>
            </div>

            <!-- Métrica de CPU -->
            <div class="card metric-card">
                <div class="metric-header">
                    <span class="metric-title">Uso de CPU</span>
                    <span id="cpuValue" class="metric-value">0%</span>
                </div>
                <div class="progress-bar-bg">
                    <div id="cpuBar" class="progress-bar-fill fill-cpu"></div>
                </div>
                <div class="metric-sub" id="cpuCores">Detectando núcleos...</div>
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

            <!-- Painel de Controle da Simulação L7 -->
            <div class="card control-card">
                <h2 style="font-size: 1.15rem; font-weight: 700;">Configurar e Disparar Teste de Carga L7</h2>
                
                <div class="form-row">
                    <div class="form-group">
                        <label for="modeSelect">Tipo de Carga da Aplicação</label>
                        <select id="modeSelect">
                            <option value="both">Carga Total (100% CPU em Todos os Núcleos + Exaustão de RAM)</option>
                            <option value="cpu">Somente CPU (Processamento Criptográfico SHA-256)</option>
                            <option value="ram">Somente RAM (Preenchimento Forçado de Páginas)</option>
                        </select>
                    </div>

                    <div class="form-group">
                        <label for="concurrencySlider">Processos Concorrentes de CPU</label>
                        <div class="slider-container">
                            <input type="range" id="concurrencySlider" min="1" max="16" value="4">
                            <span id="concurrencyVal" class="slider-val">4</span>
                        </div>
                    </div>
                </div>

                <div class="actions-row">
                    <button id="btnToggle" class="btn-start" onclick="toggleSimulation()">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
                            <path d="M8 5v14l11-7z"/>
                        </svg>
                        Iniciar Teste de Carga
                    </button>
                    <button class="btn-reset" onclick="resetMemory()">
                        Limpar Cache / Desalocar Memória
                    </button>
                </div>
            </div>

            <!-- Contador de requisições / Telemetria -->
            <div class="logs-card">
                <span>Requisições HTTP Processadas: <strong id="reqCount" style="color: #fff;">0</strong></span>
                <span>Memória no Heap da Aplicação: <strong id="appCacheMb" style="color: #fff;">0 MB</strong></span>
            </div>
        </div>
    </div>

    <script>
        let isRunning = false;
        const concurrencySlider = document.getElementById('concurrencySlider');
        const concurrencyVal = document.getElementById('concurrencyVal');
        concurrencySlider.oninput = () => { concurrencyVal.innerText = concurrencySlider.value; };

        async function fetchStats() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();

                // Atualiza CPU
                document.getElementById('cpuValue').innerText = data.cpu_percent.toFixed(1) + '%';
                document.getElementById('cpuBar').style.width = Math.min(100, data.cpu_percent) + '%';
                document.getElementById('cpuCores').innerText = `${data.cpu_count} núcleos disponíveis`;

                // Atualiza RAM
                document.getElementById('ramValue').innerText = data.ram_percent.toFixed(1) + '%';
                document.getElementById('ramBar').style.width = Math.min(100, data.ram_percent) + '%';
                document.getElementById('ramDetails').innerText = `${data.ram_used_gb.toFixed(2)} GB de ${data.ram_total_gb.toFixed(2)} GB (${data.ram_percent.toFixed(0)}%)`;

                // Atualiza SWAP
                document.getElementById('swapValue').innerText = data.swap_percent.toFixed(1) + '%';
                document.getElementById('swapBar').style.width = Math.min(100, data.swap_percent) + '%';
                document.getElementById('swapDetails').innerText = `${data.swap_used_gb.toFixed(2)} GB de ${data.swap_total_gb.toFixed(2)} GB (${data.swap_percent.toFixed(0)}%)`;

                // Status e telemetria
                document.getElementById('reqCount').innerText = data.requests_processed.toLocaleString();
                document.getElementById('appCacheMb').innerText = data.app_cache_mb + ' MB';

                isRunning = data.simulation_running;
                updateButtonUI();
            } catch (err) {
                console.error("Falha ao obter status:", err);
            }
        }

        function updateButtonUI() {
            const btn = document.getElementById('btnToggle');
            const badge = document.getElementById('statusBadge');
            const badgeText = document.getElementById('statusText');

            if (isRunning) {
                btn.className = 'btn-stop';
                btn.innerHTML = `
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
                        <rect x="6" y="6" width="12" height="12"/>
                    </svg>
                    Interromper Teste
                `;
                badge.className = 'status-badge active';
                badgeText.innerText = 'Carga Ativa na Aplicação';
            } else {
                btn.className = 'btn-start';
                btn.innerHTML = `
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M8 5v14l11-7z"/>
                    </svg>
                    Iniciar Teste de Carga
                `;
                badge.className = 'status-badge';
                badgeText.innerText = 'Sistema Ocioso';
            }
        }

        async function toggleSimulation() {
            const endpoint = isRunning ? '/api/simulation/stop' : '/api/simulation/start';
            const payload = {
                mode: document.getElementById('modeSelect').value,
                concurrency: parseInt(concurrencySlider.value, 10)
            };

            await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            fetchStats();
        }

        async function resetMemory() {
            await fetch('/api/simulation/reset', { method: 'POST' });
            fetchStats();
        }

        // Polling contínuo para manter os gráficos sincronizados a cada 1 segundo
        setInterval(fetchStats, 1000);
        fetchStats();
    </script>
</body>
</html>
"""


def cpu_stress_worker(stop_event):
    """
    Loop de computação em processo nativo independente.
    Bypassa totalmente o GIL do Python para garantir 100% de uso de CPU no núcleo alocado.
    """
    while not stop_event.is_set():
        _ = 123456789 * 987654321


def ram_stress_loop(stop_event, target_percent=96):
    """
    Aloca e escreve páginas ativamente na memória até atingir a porcentagem alvo.
    Usa bytearray preenchido com 0xFF para forçar o kernel a mapear memória física real
    e evitar o descarte por páginas zeradas.
    """
    chunk_size = 40 * 1024 * 1024  # 40 MB
    while not stop_event.is_set():
        mem = psutil.virtual_memory()
        if mem.percent >= target_percent:
            time.sleep(0.3)
            continue
        try:
            chunk = bytearray(b"\xff" * chunk_size)
            with state_lock:
                system_state["allocated_ram_chunks"].append(chunk)
                system_state["requests_processed"] += 1
            time.sleep(0.01)
        except MemoryError:
            time.sleep(0.5)


class AppRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Desativa logs no terminal para economizar I/O durante o teste
        return

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/" or parsed.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_DASHBOARD.encode("utf-8"))
            return

        elif parsed.path == "/api/stats":
            # Coleta de métricas via psutil
            cpu_pct = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            swap = psutil.swap_memory()

            with state_lock:
                cache_size = sum(len(c) for c in system_state["allocated_ram_chunks"])
                payload = {
                    "cpu_percent": cpu_pct,
                    "cpu_count": psutil.cpu_count(logical=True),
                    "ram_percent": mem.percent,
                    "ram_used_gb": (mem.total - mem.available) / (1024**3),
                    "ram_total_gb": mem.total / (1024**3),
                    "swap_percent": swap.percent,
                    "swap_used_gb": swap.used / (1024**3),
                    "swap_total_gb": swap.total / (1024**3),
                    "simulation_running": system_state["simulation_running"],
                    "requests_processed": system_state["requests_processed"],
                    "app_cache_mb": cache_size // (1024 * 1024),
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

        if parsed.path == "/api/simulation/start":
            mode = data.get("mode", "both")
            concurrency = int(data.get("concurrency", 4))

            with state_lock:
                if not system_state["simulation_running"]:
                    system_state["simulation_running"] = True
                    system_state["simulation_mode"] = mode
                    system_state["concurrency"] = concurrency
                    system_state["stop_event"].clear()

                    # 1. Processos dedicados para CPU (100% real sem GIL)
                    if mode in ("cpu", "both"):
                        num_cpus = concurrency
                        for _ in range(num_cpus):
                            p = multiprocessing.Process(
                                target=cpu_stress_worker,
                                args=(system_state["stop_event"],),
                                daemon=True,
                            )
                            p.start()
                            system_state["cpu_processes"].append(p)

                    # 2. Thread agressiva para preenchimento de RAM
                    if mode in ("ram", "both"):
                        t = threading.Thread(
                            target=ram_stress_loop,
                            args=(system_state["stop_event"],),
                            daemon=True,
                        )
                        t.start()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "started"}).encode("utf-8"))
            return

        elif parsed.path == "/api/simulation/stop":
            with state_lock:
                system_state["simulation_running"] = False
                system_state["stop_event"].set()

                # Encerra processos de CPU imediatamente
                for p in system_state["cpu_processes"]:
                    if p.is_alive():
                        p.terminate()
                system_state["cpu_processes"].clear()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "stopped"}).encode("utf-8"))
            return

        elif parsed.path == "/api/simulation/reset":
            with state_lock:
                system_state["allocated_ram_chunks"].clear()
                system_state["requests_processed"] = 0
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
    print("  LABORATÓRIO DE CARGA E RESILIÊNCIA - CAMADA DE APLICAÇÃO")
    print("=" * 65)
    print(f"[*] Servidor rodando em: http://localhost:{port}")
    print(f"[*] Acesse pelo navegador na VM ou na sua máquina host.")
    print("    Pressione [Ctrl + C] no terminal para encerrar.")
    print("=" * 65)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[!] Encerrando servidor...")
        httpd.shutdown()


if __name__ == "__main__":
    multiprocessing.set_start_method("fork", force=False)
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    run_server(port)
