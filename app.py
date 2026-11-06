#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Servidor Alvo — Painel de Monitoramento de Resiliência (Camada de Aplicação - L7).
Projetado para atuar exclusivamente como VÍTIMA e PAINEL DE TELEMETRIA no laboratório de redes.
Todo o tráfego e estresse de CPU/RAM são recebidos externamente da Máquina B.
Compatível com Python 3.4 / 3.5+ e com fallback nativo Linux (/proc) caso psutil não esteja instalado.
"""

import concurrent.futures
import gc
import hashlib
import json
import mmap
import os
import socket
import sys
import threading
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
import urllib.parse

# Reduz stack de cada thread de 8MB (padrao Linux) para 512KB para economizar memoria virtual
try:
    threading.stack_size(512 * 1024)
except Exception:
    pass

# Define prioridade maxima (1000) no OOM Killer do Linux para que o kernel mate
# este processo imediatamente quando a RAM+Swap acabar (protegendo o SSH e o SO)
try:
    with open("/proc/self/oom_score_adj", "w") as _oom_f:
        _oom_f.write("1000\n")
except Exception:
    pass

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

CPU_COUNT = os.cpu_count() or 2

# Estado para cálculo nativo de CPU via /proc/stat caso psutil não esteja instalado
_prev_proc_cpu = {"idle": 0, "total": 0}


def get_system_metrics():
    """Retorna (cpu_pct, ram_pct, ram_used_gb, ram_total_gb, swap_pct, swap_used_gb, swap_total_gb)."""
    if HAS_PSUTIL:
        cpu_pct = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        return (
            cpu_pct,
            mem.percent,
            (mem.total - mem.available) / (1024.0 ** 3),
            mem.total / (1024.0 ** 3),
            swap.percent,
            swap.used / (1024.0 ** 3),
            swap.total / (1024.0 ** 3),
        )

    # Fallback nativo via /proc/stat e /proc/meminfo (Linux sem psutil)
    cpu_pct = 0.0
    try:
        with open("/proc/stat", "r") as f:
            line = f.readline()
        parts = [float(x) for x in line.split()[1:]]
        idle = parts[3] + (parts[4] if len(parts) > 4 else 0.0)
        total = sum(parts)
        diff_total = total - _prev_proc_cpu["total"]
        diff_idle = idle - _prev_proc_cpu["idle"]
        _prev_proc_cpu["total"] = total
        _prev_proc_cpu["idle"] = idle
        if diff_total > 0:
            cpu_pct = max(0.0, min(100.0, 100.0 * (1.0 - diff_idle / diff_total)))
    except Exception:
        pass

    ram_pct, ram_used_gb, ram_total_gb = 0.0, 0.0, 1.0
    swap_pct, swap_used_gb, swap_total_gb = 0.0, 0.0, 0.0
    try:
        meminfo = {}
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if ":" in line:
                    k, v = line.split(":", 1)
                    meminfo[k.strip()] = int(v.strip().split()[0]) * 1024  # bytes
        mem_total = meminfo.get("MemTotal", 0)
        mem_avail = meminfo.get(
            "MemAvailable",
            meminfo.get("MemFree", 0) + meminfo.get("Buffers", 0) + meminfo.get("Cached", 0),
        )
        if mem_total > 0:
            mem_used = max(0, mem_total - mem_avail)
            ram_pct = (mem_used / float(mem_total)) * 100.0
            ram_used_gb = mem_used / (1024.0 ** 3)
            ram_total_gb = mem_total / (1024.0 ** 3)

        swap_total = meminfo.get("SwapTotal", 0)
        swap_free = meminfo.get("SwapFree", 0)
        if swap_total > 0:
            swap_used = max(0, swap_total - swap_free)
            swap_pct = (swap_used / float(swap_total)) * 100.0
            swap_used_gb = swap_used / (1024.0 ** 3)
            swap_total_gb = swap_total / (1024.0 ** 3)
    except Exception:
        pass

    return (
        cpu_pct,
        ram_pct,
        ram_used_gb,
        ram_total_gb,
        swap_pct,
        swap_used_gb,
        swap_total_gb,
    )


def get_process_rss_mb():
    """Retorna a memoria residente atual deste processo em MB no Linux."""
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except (IOError, OSError, ValueError, IndexError):
        pass
    return 0


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
    "network_requests_completed": 0,
    "memory_allocation_failures": 0,
    "allocated_ram_chunks": [],
    "virtual_oom_reserve": [],
    "allocated_ram_bytes": 0,
    "last_request_time": 0.0,
    "start_time": time.time(),
}

state_lock = threading.Lock()


class PooledHTTPServer(HTTPServer):
    """
    Servidor HTTP com Pool Fixo de Threads pre-alocadas.
    Evita 'RuntimeError: can't start new thread' mesmo com 100% da RAM ocupada.
    """
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128

    def __init__(self, server_address, RequestHandlerClass, max_workers=64):
        HTTPServer.__init__(self, server_address, RequestHandlerClass)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        # Pre-aquece todas as threads do pool no inicio (antes da RAM encher)
        futures = [self.executor.submit(lambda: None) for _ in range(max_workers)]
        concurrent.futures.wait(futures)

    def process_request_thread(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            pass
        finally:
            self.shutdown_request(request)

    def process_request(self, request, client_address):
        try:
            self.executor.submit(self.process_request_thread, request, client_address)
        except Exception:
            self.shutdown_request(request)

    def handle_error(self, request, client_address):
        # Silencia tracebacks de sockets desconectados no terminal
        pass


HTML_DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Servidor Alvo — Telemetria de Resiliência L7</title>
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
            flex-wrap: wrap;
            gap: 1rem;
        }

        .header-title h1 {
            font-size: 1.75rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, var(--accent-cyan), var(--accent-blue));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .ip-badge {
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
        }

        .status-badge {
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
        }

        .status-badge.active {
            background: rgba(255, 65, 108, 0.18);
            color: var(--accent-red);
            border-color: rgba(255, 65, 108, 0.5);
            animation: pulse 1.2s infinite;
        }

        .status-dot {
            width: 9px;
            height: 9px;
            border-radius: 50%;
            background-color: currentColor;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.7; transform: scale(1.2); }
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
            font-size: 2.1rem;
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

        .info-card {
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
        }

        .info-text h3 {
            font-size: 1rem;
            font-weight: 700;
            color: #38bdf8;
            margin-bottom: 0.25rem;
        }

        .info-text p {
            font-size: 0.85rem;
            color: #cbd5e1;
            line-height: 1.5;
        }

        .btn-reset {
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
        }

        .btn-reset:hover {
            background: rgba(255, 255, 255, 0.16);
            border-color: rgba(255, 255, 255, 0.4);
        }

        .logs-card {
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
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-title">
                <h1>Painel de Telemetria — Servidor Alvo (L7)</h1>
                <div class="ip-badge">
                    📡 Escutando na Rede: <strong>http://__LOCAL_IP__:8080</strong>
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
                <div class="metric-sub" id="cpuCores">__CPU_COUNT__ núcleos detectados</div>
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
                        Para disparar o teste, execute o <code>client_flood.py</code> na <strong>Máquina B (Atacante)</strong> apontando para <strong>http://__LOCAL_IP__:8080</strong>.
                    </p>
                </div>
                <button class="btn-reset" onclick="resetMemory()">
                    🧹 Limpar Memória / Resetar Contadores
                </button>
            </div>

            <!-- Telemetria em Tempo Real -->
            <div class="logs-card">
                <span>L7 Aceitas / Concluídas: <strong id="netReqCount" style="color: #38bdf8;">0 / 0</strong></span>
                <span>Memória Retida no Heap: <strong id="appCacheMb" style="color: #fff;">0 MB</strong></span>
                <span>RAM Residente do Processo: <strong id="processRssMb" style="color: #fff;">0 MB</strong></span>
                <span>Tempo Ativo: <strong id="uptimeVal" style="color: #fff;">0s</strong></span>
            </div>
        </div>
    </div>

    <script>
        async function fetchStats() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();

                // Atualiza CPU
                document.getElementById('cpuValue').innerText = data.cpu_percent.toFixed(1) + '%';
                document.getElementById('cpuBar').style.width = Math.min(100, data.cpu_percent) + '%';
                document.getElementById('cpuCores').innerText = data.cpu_count + ' núcleos detectados';

                // Atualiza RAM
                document.getElementById('ramValue').innerText = data.ram_percent.toFixed(1) + '%';
                document.getElementById('ramBar').style.width = Math.min(100, data.ram_percent) + '%';
                document.getElementById('ramDetails').innerText = data.ram_used_gb.toFixed(2) + ' GB de ' + data.ram_total_gb.toFixed(2) + ' GB (' + data.ram_percent.toFixed(0) + '%)';

                // Atualiza SWAP
                document.getElementById('swapValue').innerText = data.swap_percent.toFixed(1) + '%';
                document.getElementById('swapBar').style.width = Math.min(100, data.swap_percent) + '%';
                document.getElementById('swapDetails').innerText = data.swap_used_gb.toFixed(2) + ' GB de ' + data.swap_total_gb.toFixed(2) + ' GB (' + data.swap_percent.toFixed(0) + '%)';

                // Telemetria de Rede
                document.getElementById('netReqCount').innerText =
                    (data.network_requests_received || 0).toLocaleString() + ' / ' +
                    (data.network_requests_completed || 0).toLocaleString();
                document.getElementById('appCacheMb').innerText = data.app_cache_mb + ' MB';
                document.getElementById('processRssMb').innerText = data.process_rss_mb + ' MB';
                document.getElementById('uptimeVal').innerText = data.uptime_sec + 's';

                // Atualiza badge de tráfego ativo
                const badge = document.getElementById('statusBadge');
                const badgeText = document.getElementById('statusText');
                if (data.is_under_attack) {
                    badge.className = 'status-badge active';
                    badgeText.innerText = '🚨 Sob Tráfego L7 Ativo';
                } else {
                    badge.className = 'status-badge';
                    badgeText.innerText = 'Aguardando Tráfego da Rede';
                }
            } catch (err) {
                console.error("Falha ao obter status:", err);
            }
        }

        async function resetMemory() {
            await fetch('/api/reset', { method: 'POST' });
            fetchStats();
        }

        setInterval(fetchStats, 1000);
        fetchStats();
    </script>
</body>
</html>
"""

HTML_DASHBOARD = (
    HTML_DASHBOARD_TEMPLATE
    .replace("__LOCAL_IP__", str(LOCAL_IP))
    .replace("__CPU_COUNT__", str(CPU_COUNT))
)


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
            (
                cpu_pct,
                ram_pct,
                ram_used_gb,
                ram_total_gb,
                swap_pct,
                swap_used_gb,
                swap_total_gb,
            ) = get_system_metrics()

            now = time.time()
            with state_lock:
                cache_size = server_state["allocated_ram_bytes"]
                is_under_attack = (now - server_state["last_request_time"]) < 2.5
                uptime = int(now - server_state["start_time"])

                payload = {
                    "cpu_percent": cpu_pct,
                    "cpu_count": CPU_COUNT,
                    "ram_percent": ram_pct,
                    "ram_used_gb": ram_used_gb,
                    "ram_total_gb": ram_total_gb,
                    "swap_percent": swap_pct,
                    "swap_used_gb": swap_used_gb,
                    "swap_total_gb": swap_total_gb,
                    "network_requests_received": server_state["network_requests_received"],
                    "network_requests_completed": server_state["network_requests_completed"],
                    "memory_allocation_failures": server_state["memory_allocation_failures"],
                    "app_cache_mb": cache_size // (1024 * 1024),
                    "process_rss_mb": get_process_rss_mb(),
                    "is_under_attack": is_under_attack,
                    "uptime_sec": uptime,
                }

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode("utf-8"))
            return

        self.send_error(404, "Endpoint nao encontrado")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"

        try:
            data = json.loads(body.decode("utf-8")) if body else {}
        except ValueError:
            data = {}

        # Endpoint autêntico de Camada 7 para receber tráfego da Máquina Atacante
        if parsed.path == "/api/workload":
            now = time.time()
            with state_lock:
                # "Recebida" significa que o servidor aceitou e leu o POST;
                # o processamento pesado ainda pode estar em andamento.
                server_state["network_requests_received"] += 1
                server_state["last_request_time"] = now

            mode = data.get("mode", "both")
            rounds = int(data.get("rounds", 250000))
            chunk_mb = int(data.get("chunk_mb", 40))

            start_t = time.time()

            # 1. Carga de CPU sob demanda (pbkdf2_hmac libera o GIL do Python e usa 100% de todos os nucleos)
            if mode in ("cpu", "both"):
                hashlib.pbkdf2_hmac(
                    "sha256",
                    b"academic_l7_workload_payload",
                    b"telemetry_salt",
                    rounds,
                )

            # 2. Carga de RAM sob demanda sem limite (vai ate 100% da RAM + SWAP e aciona o OOM Killer)
            if mode in ("ram", "both"):
                size_bytes = chunk_mb * 1024 * 1024

                # Pre-mapeia reserva de memoria virtual (0 MB de RAM fisica ate ser tocada).
                # Quando a RAM chegar perto de 98% e o malloc bloquear novos bytearrays com MemoryError,
                # sujar essas paginas ja mapeadas forca Page Fault real no kernel e dispara o OOM Killer!
                with state_lock:
                    if not server_state["virtual_oom_reserve"] and server_state["memory_allocation_failures"] == 0:
                        try:
                            for _ in range(32):
                                server_state["virtual_oom_reserve"].append(
                                    mmap.mmap(-1, 32 * 1024 * 1024)
                                )
                        except Exception:
                            pass

                try:
                    chunk = bytearray(size_bytes)
                    for offset in range(0, size_bytes, 4096):
                        chunk[offset] = 1
                    with state_lock:
                        server_state["allocated_ram_chunks"].append(chunk)
                        server_state["allocated_ram_bytes"] += len(chunk)
                except MemoryError:
                    with state_lock:
                        server_state["memory_allocation_failures"] += 1

                    # 2a. Tenta alocar blocos menores (ate 512 KB) para esgotar o malloc sem travar o state_lock
                    fallback_size = size_bytes // 2
                    while fallback_size >= 524288:
                        try:
                            small_chunk = bytearray(fallback_size)
                            for offset in range(0, fallback_size, 4096):
                                small_chunk[offset] = 1
                            with state_lock:
                                server_state["allocated_ram_chunks"].append(small_chunk)
                                server_state["allocated_ram_bytes"] += len(small_chunk)
                        except MemoryError:
                            fallback_size //= 2

                    # 2b. Toca nas paginas da reserva virtual pre-mapeada para forcar Page Fault
                    # nos ultimos megabytes da RAM fisica e acionar o OOM Killer do Linux (SIGKILL)
                    while True:
                        with state_lock:
                            if not server_state["virtual_oom_reserve"]:
                                break
                            vchunk = server_state["virtual_oom_reserve"].pop()
                        vsize = len(vchunk)
                        for offset in range(0, vsize, 4096):
                            vchunk[offset:offset + 1] = b"\x01"
                        with state_lock:
                            server_state["allocated_ram_chunks"].append(vchunk)
                            server_state["allocated_ram_bytes"] += vsize

            now = time.time()
            with state_lock:
                server_state["network_requests_completed"] += 1
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
                for chunk in server_state["virtual_oom_reserve"]:
                    try:
                        chunk.close()
                    except Exception:
                        pass
                del server_state["virtual_oom_reserve"][:]
                del server_state["allocated_ram_chunks"][:]
                server_state["network_requests_received"] = 0
                server_state["network_requests_completed"] = 0
                server_state["memory_allocation_failures"] = 0
                server_state["allocated_ram_bytes"] = 0
                gc.collect()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "reset"}).encode("utf-8"))
            return

        self.send_error(404, "Endpoint nao encontrado")


def run_server(port=8080):
    get_system_metrics()

    server_address = ("0.0.0.0", port)
    httpd = PooledHTTPServer(server_address, AppRequestHandler, max_workers=64)
    print("=" * 65)
    print("  SERVIDOR ALVO — MONITORAMENTO DE RESILIÊNCIA (L7)")
    print("=" * 65)
    print("[*] Nucleos de CPU detectados: {0}".format(CPU_COUNT))
    print("[*] Coletor de Telemetria:     {0}".format("psutil" if HAS_PSUTIL else "Nativo Linux (/proc)"))
    print("[*] IP na Rede Local:          http://{0}:{1}".format(LOCAL_IP, port))
    print("[*] Endpoint de Carga L7:      http://{0}:{1}/api/workload".format(LOCAL_IP, port))
    print("=" * 65)
    print("[*] Aguardando trafego do client_flood.py da Maquina B...")
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
